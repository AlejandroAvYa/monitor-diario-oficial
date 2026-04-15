
"""
scraper.py — Monitor Diario Oficial Chile (v4)
División de Seguridad Privada (DSP)

Modos de ejecución:
  python scraper.py                        → Escaneo del día de hoy
  python scraper.py --historical           → Escaneo desde 15-02-2025 hasta hoy
  python scraper.py --date 13-04-2026      → Escaneo de una fecha específica
  python scraper.py --test                 → Diagnóstico: verifica cada sección con edición conocida
  python scraper.py --test --date 13-04-2026  → Diagnóstico de una fecha específica

Estrategia de búsqueda de edición (v4):
  - HEAD request al PDF del sumario: /publicaciones/YYYY/MM/DD/sumarios/{edition}.pdf
  - HEAD no descarga el cuerpo, solo verifica existencia (HTTP 200 / 404)
  - Búsqueda desde la estimación hacia afuera (±75), alternando + y -
  - Anclajes múltiples que se enriquecen automáticamente con cada edición encontrada

Nota sobre robots.txt:
  El sitio del DO restringe crawlers automáticos en su robots.txt.
  La librería requests de Python no verifica robots.txt por defecto,
  por lo que el scraper accede normalmente usando un User-Agent de navegador.
"""

import json
import os
import re
import time
import smtplib
import logging
import argparse
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────

BASE_DIR      = Path(__file__).parent
DATA_FILE     = BASE_DIR / "data" / "publications.json"
KEYWORDS_FILE = BASE_DIR / "keywords.json"

BASE_URL    = "https://www.diariooficial.interior.gob.cl/edicionelectronica"
SUMARIO_URL = "https://www.diariooficial.interior.gob.cl/publicaciones/{yyyy}/{mm}/{dd}/sumarios/{edition}.pdf"
START_DATE  = date(2025, 2, 15)
REQUEST_DELAY = 1.2   # segundos entre requests de scraping (respetar el servidor)

# Anclajes conocidos y verificados (fecha → número de edición)
# El scraper enriquece esta tabla automáticamente con cada edición encontrada
ANCHORS: dict[date, int] = {
    date(2026, 4, 13): 44423,   # verificado directamente del sitio
    date(2025, 8, 15): 44150,   # estimado para reducir desvío en mitad del período
    date(2025, 2, 15): 43960,   # estimado para inicio del período
}

# Las 4 secciones a scrapear (las 3 restantes están excluidas por instrucción)
SECTIONS: dict[str, str] = {
    "Normas Generales":         "index.php",
    "Normas Particulares":      "normas_particulares.php",
    "Publicaciones Judiciales": "publicaciones_judiciales.php",
    "Avisos Destacados":        "avisos_destacados.php",
}

# Secciones excluidas (existen en el DO pero no son relevantes para DSP)
SECTIONS_EXCLUDED = [
    "Empresas y Cooperativas",
    "Marcas y Patentes",
    "Boletín Oficial de Minería",
]

# Credenciales desde GitHub Secrets (nunca hardcodeadas aquí)
GMAIL_USER     = os.environ.get("GMAIL_USER", "")
GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL   = os.environ.get("NOTIFY_EMAIL", GMAIL_USER)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# CARGA Y GUARDADO DE DATOS
# ──────────────────────────────────────────────────────────────────────────────

def load_data() -> dict:
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "last_updated":   None,
        "total":          0,
        "editions_cache": {},
        "publications":   [],
        "skipped_dates":  [],
    }


def save_data(data: dict):
    data["total"]        = len(data["publications"])
    data["last_updated"] = date.today().isoformat()
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(
        f"Guardado: {data['total']} publicaciones · "
        f"{len(data.get('skipped_dates', []))} fechas sin edición."
    )


def load_keywords() -> dict:
    with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
        kw = json.load(f)
    return {k: kw.get(k, []) for k in ["alta_prioridad", "instituciones", "materias"]}


def get_processed_dates(data: dict) -> set:
    return (
        {p["date"] for p in data.get("publications", [])}
        | set(data.get("skipped_dates", []))
    )


# ──────────────────────────────────────────────────────────────────────────────
# EDITION FINDER — HEAD requests al sumario PDF
# ──────────────────────────────────────────────────────────────────────────────

def sumario_exists(session: requests.Session, target: date, edition_id: int) -> bool:
    """
    Verifica con HEAD si existe el PDF de sumario para esa edición y fecha.
    HEAD no descarga el cuerpo del archivo: solo obtiene el código HTTP.
    - HTTP 200 → edición existe para esa fecha ✓
    - HTTP 404 → no existe para esa fecha ✗
    """
    url = SUMARIO_URL.format(
        yyyy=target.strftime("%Y"),
        mm=target.strftime("%m"),
        dd=target.strftime("%d"),
        edition=edition_id,
    )
    try:
        r = session.head(url, timeout=8, allow_redirects=True)
        return r.status_code == 200
    except Exception:
        return False


def estimate_edition(target: date, cache: dict) -> int:
    """
    Estima el número de edición para una fecha usando el anclaje más cercano.
    Combina los anclajes hardcodeados (ANCHORS) con los ya descubiertos (cache).
    """
    known = dict(ANCHORS)
    for date_str, eid in cache.items():
        try:
            d = date(int(date_str[6:10]), int(date_str[3:5]), int(date_str[:2]))
            known[d] = eid
        except Exception:
            pass
    nearest = min(known.keys(), key=lambda d: abs((target - d).days))
    return known[nearest] + (target - nearest).days


def find_edition(
    session: requests.Session,
    target: date,
    cache: dict,
    verbose: bool = False,
) -> int | None:
    """
    Encuentra el número de edición exacto para una fecha.
    Búsqueda lineal desde la estimación hacia afuera (±75), alternando + y -.
    """
    date_str  = target.strftime("%d-%m-%Y")

    # 1. Caché
    if date_str in cache:
        if verbose:
            log.info(f"  [caché] {date_str} → #{cache[date_str]}")
        return cache[date_str]

    estimated = estimate_edition(target, cache)
    if verbose:
        log.info(f"  Estimación para {date_str}: #{estimated}")

    for offset in range(0, 76):
        for sign in ([0] if offset == 0 else [1, -1]):
            candidate = estimated + offset * sign
            if candidate < 1:
                continue
            if sumario_exists(session, target, candidate):
                cache[date_str] = candidate
                ANCHORS[target] = candidate   # enriquecer para futuras estimaciones
                log.info(
                    f"Edición encontrada: {date_str} → #{candidate} "
                    f"(estimación fue #{estimated}, offset real {offset * sign:+d})"
                )
                return candidate

    log.warning(f"{date_str}: sin edición en ±75 (feriado, día no hábil o error de red)")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# SCRAPING DE SECCIONES
# ──────────────────────────────────────────────────────────────────────────────

def scrape_section(
    session: requests.Session,
    url: str,
    section_name: str,
    verbose: bool = False,
) -> list[dict]:
    """
    Scrapea una sección del DO y extrae todos los items (título + CVE + PDF URL).
    Estructura esperada: tabla HTML con filas [título | Ver PDF (CVE-XXXXXXX)].
    """
    items = []
    try:
        r = session.get(url, timeout=25)

        if verbose:
            log.info(f"    HTTP {r.status_code} — {len(r.text)} bytes recibidos")

        if r.status_code != 200:
            log.warning(f"    {section_name}: HTTP {r.status_code} en {url}")
            return []

        soup  = BeautifulSoup(r.text, "html.parser")
        rows  = soup.select("table tr")

        if verbose:
            log.info(f"    Filas encontradas en tabla: {len(rows)}")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            title = cells[0].get_text(separator=" ", strip=True)
            link  = cells[-1].find("a")

            # Saltar filas de encabezado o vacías
            if not title or not link or len(title) < 10:
                continue

            href      = link.get("href", "")
            link_text = link.get_text(strip=True)

            # Extraer CVE del texto o de la URL
            m   = re.search(r'CVE-?(\d+)', link_text + href)
            cve = m.group(1) if m else re.sub(r'[^a-zA-Z0-9]', '', href)[-12:]

            pdf_url = (
                href if href.startswith("http")
                else f"https://www.diariooficial.interior.gob.cl{href}"
            )

            items.append({
                "title":   title,
                "pdf_url": pdf_url,
                "cve":     cve or href,
            })

    except Exception as e:
        log.error(f"    Error scrapeando {section_name} ({url}): {e}")

    return items


# ──────────────────────────────────────────────────────────────────────────────
# FILTRO DE KEYWORDS
# ──────────────────────────────────────────────────────────────────────────────

def check_keywords(text: str, keywords: dict) -> tuple[list, str]:
    """
    Aplica todas las keywords al texto del título.
    Retorna (lista de keywords encontradas, prioridad: 'alta' o 'normal').
    """
    text_lower = text.lower()
    matched    = []
    priority   = "normal"

    for kw in keywords["alta_prioridad"]:
        if kw.lower() in text_lower:
            matched.append(kw)
            priority = "alta"

    for group in ["instituciones", "materias"]:
        for kw in keywords[group]:
            if kw.lower() in text_lower and kw not in matched:
                matched.append(kw)

    return matched, priority


# ──────────────────────────────────────────────────────────────────────────────
# PROCESAMIENTO DE UN DÍA COMPLETO
# ──────────────────────────────────────────────────────────────────────────────

def process_date(
    session: requests.Session,
    target: date,
    data: dict,
    keywords: dict,
    verbose: bool = False,
) -> list[dict]:
    """
    Procesa una fecha completa:
    1. Encuentra el número de edición
    2. Scrapea las 4 secciones relevantes
    3. Filtra por keywords
    4. Guarda matches en data["publications"]
    """
    date_str  = target.strftime("%d-%m-%Y")
    processed = get_processed_dates(data)

    if date_str in processed:
        log.debug(f"{date_str}: ya procesado anteriormente.")
        return []

    # ── Encontrar edición ──────────────────────────────────────────────────────
    edition_id = find_edition(
        session, target,
        data.setdefault("editions_cache", {}),
        verbose=verbose,
    )
    if not edition_id:
        skipped = data.setdefault("skipped_dates", [])
        if date_str not in skipped:
            skipped.append(date_str)
        return []

    log.info(f"Procesando {date_str} (ed. #{edition_id}) — 4 secciones…")

    new_matches   = []
    existing_cves = {p["cve"] for p in data["publications"]}

    # ── Scrapear cada sección ─────────────────────────────────────────────────
    for section_name, php_file in SECTIONS.items():
        url   = f"{BASE_URL}/{php_file}?date={date_str}&edition={edition_id}"
        items = scrape_section(session, url, section_name, verbose=verbose)

        if verbose:
            log.info(f"    → {section_name}: {len(items)} item(s) encontrados")

        for item in items:
            matched_kw, priority = check_keywords(item["title"], keywords)
            if matched_kw and item["cve"] not in existing_cves:
                pub = {
                    "cve":        item["cve"],
                    "date":       date_str,
                    "edition_id": edition_id,
                    "section":    section_name,
                    "title":      item["title"],
                    "pdf_url":    item["pdf_url"],
                    "matched_kw": matched_kw,
                    "priority":   priority,
                    "notified":   False,
                }
                data["publications"].append(pub)
                existing_cves.add(item["cve"])
                new_matches.append(pub)
                log.info(
                    f"  ✓ [{priority.upper()}] {section_name}: "
                    f"{item['title'][:80]}"
                )
                if verbose:
                    log.info(f"     Keywords: {matched_kw}")
                    log.info(f"     PDF: {item['pdf_url']}")

        time.sleep(REQUEST_DELAY)

    return new_matches


# ──────────────────────────────────────────────────────────────────────────────
# MODO DIAGNÓSTICO
# ──────────────────────────────────────────────────────────────────────────────

def run_diagnostic(session: requests.Session, target: date, keywords: dict):
    """
    Verifica el acceso y parseo de cada sección para una fecha dada.
    Muestra los primeros 5 títulos de cada sección y los matches de keywords.
    NO guarda nada — solo informa.
    """
    print("\n" + "═" * 70)
    print(f"  DIAGNÓSTICO — Diario Oficial del {target.strftime('%d-%m-%Y')}")
    print("═" * 70)

    # 1. Verificar edición
    print(f"\n[1/6] Buscando número de edición…")
    cache = {}
    edition_id = find_edition(session, target, cache, verbose=True)
    if not edition_id:
        print(f"  ✗ No se encontró edición para {target.strftime('%d-%m-%Y')}")
        print("    → Posible causa: día no hábil, feriado o error de red.")
        return

    print(f"  ✓ Edición #{edition_id} encontrada\n")

    # 2. Verificar sumario PDF
    print(f"[2/6] Verificando sumario PDF…")
    sumario = SUMARIO_URL.format(
        yyyy=target.strftime("%Y"), mm=target.strftime("%m"),
        dd=target.strftime("%d"),  edition=edition_id,
    )
    print(f"  URL: {sumario}")
    if sumario_exists(session, target, edition_id):
        print(f"  ✓ Sumario PDF accesible (HTTP 200)\n")
    else:
        print(f"  ✗ Sumario PDF no accesible\n")

    # 3. Scrapear cada sección
    total_items   = 0
    total_matches = 0

    for i, (section_name, php_file) in enumerate(SECTIONS.items(), start=3):
        date_str = target.strftime("%d-%m-%Y")
        url      = f"{BASE_URL}/{php_file}?date={date_str}&edition={edition_id}"

        print(f"[{i}/6] Sección: {section_name}")
        print(f"  URL: {url}")

        try:
            r = session.get(url, timeout=25)
            print(f"  HTTP: {r.status_code} — {len(r.text):,} bytes")

            if r.status_code != 200:
                print(f"  ✗ No accesible\n")
                continue

            soup  = BeautifulSoup(r.text, "html.parser")
            items = scrape_section(session, url, section_name)

            print(f"  Items encontrados: {len(items)}")
            total_items += len(items)

            # Mostrar primeros 5 títulos
            if items:
                print(f"  Primeros títulos:")
                for item in items[:5]:
                    print(f"    · {item['title'][:90]}")
                    matched_kw, priority = check_keywords(item["title"], keywords)
                    if matched_kw:
                        print(f"      ★ MATCH [{priority.upper()}]: {matched_kw}")
                        total_matches += 1
                if len(items) > 5:
                    print(f"    … y {len(items) - 5} más")
            else:
                print(f"  (sección vacía para esta fecha)")

        except Exception as e:
            print(f"  ✗ Error: {e}")

        print()
        time.sleep(1.0)

    # 4. Resumen
    print("═" * 70)
    print(f"  RESUMEN DEL DIAGNÓSTICO")
    print(f"  Fecha:     {target.strftime('%d-%m-%Y')}")
    print(f"  Edición:   #{edition_id}")
    print(f"  Secciones: {len(SECTIONS)} verificadas")
    print(f"  Items:     {total_items} en total")
    print(f"  Matches:   {total_matches} con keywords")
    print(f"\n  Secciones EXCLUIDAS (no procesadas):")
    for s in SECTIONS_EXCLUDED:
        print(f"    · {s}")
    print("═" * 70 + "\n")


# ──────────────────────────────────────────────────────────────────────────────
# NOTIFICACIONES EMAIL
# ──────────────────────────────────────────────────────────────────────────────

def send_email_alert(new_pubs: list[dict]):
    if not GMAIL_USER or not GMAIL_PASSWORD or not new_pubs:
        if new_pubs and not GMAIL_USER:
            log.warning("Credenciales Gmail no configuradas (GMAIL_USER vacío).")
        return

    alta   = [p for p in new_pubs if p["priority"] == "alta"]
    normal = [p for p in new_pubs if p["priority"] == "normal"]
    hoy    = date.today().strftime("%d/%m/%Y")

    subject = (
        f"📋 Diario Oficial [{hoy}] — "
        f"{len(alta)} alta prioridad · {len(normal)} normal"
    )

    hs = (
        "background:#f7f8fa;color:#555;font-size:11px;font-weight:600;"
        "padding:8px 10px;border-bottom:2px solid #ddd;text-align:left;"
        "text-transform:uppercase"
    )

    def rows(pubs: list[dict]) -> str:
        return "".join(
            f'<tr>'
            f'<td style="padding:8px;border-bottom:1px solid #eee;font-size:12px;'
            f'color:#555;white-space:nowrap">{p["date"]}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #eee;font-size:11px;'
            f'color:#888">{p["section"]}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #eee;font-size:12.5px">'
            f'{p["title"]}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #eee;font-size:11px;'
            f'color:#1a6ab1">{", ".join(p["matched_kw"])}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #eee">'
            f'<a href="{p["pdf_url"]}" style="color:#c0392b;font-size:12px;'
            f'font-weight:700">PDF →</a></td>'
            f'</tr>'
            for p in pubs
        )

    def table(title: str, color: str, pubs: list[dict]) -> str:
        if not pubs:
            return ""
        return (
            f'<h3 style="color:{color};font-size:14px;margin-bottom:12px">{title}</h3>'
            f'<table width="100%" cellspacing="0" style="border-collapse:collapse">'
            f'<tr>'
            f'<th style="{hs}">Fecha</th><th style="{hs}">Sección</th>'
            f'<th style="{hs}">Título</th><th style="{hs}">Keywords</th>'
            f'<th style="{hs}">PDF</th>'
            f'</tr>{rows(pubs)}</table><br>'
        )

    html = (
        f'<html><body style="font-family:Arial,sans-serif;color:#333;'
        f'max-width:900px;margin:0 auto">'
        f'<div style="background:#0d2340;padding:20px 28px;border-radius:8px 8px 0 0">'
        f'<div style="font-size:10px;font-weight:700;letter-spacing:2px;color:#e8a020;'
        f'text-transform:uppercase;margin-bottom:6px">'
        f'Subsecretaría de Prevención del Delito · DSP</div>'
        f'<h2 style="color:#fff;margin:0;font-size:18px">Monitor Diario Oficial</h2>'
        f'<p style="color:rgba(255,255,255,0.5);margin:4px 0 0;font-size:13px">'
        f'{hoy} · {len(new_pubs)} nueva(s) publicación(es)</p></div>'
        f'<div style="background:#fff;border:1px solid #e0e4ea;border-top:none;'
        f'padding:24px;border-radius:0 0 8px 8px">'
        f'{table("⚠️ Alta Prioridad", "#c0392b", alta)}'
        f'{table("📌 Prioridad Normal", "#1a6ab1", normal)}'
        f'<hr style="border:none;border-top:1px solid #eee;margin:24px 0">'
        f'<p style="font-size:11px;color:#aaa;margin:0">'
        f'Monitor automático DSP · {hoy}</p>'
        f'</div></body></html>'
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Monitor DSP <{GMAIL_USER}>"
    msg["To"]      = NOTIFY_EMAIL
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(GMAIL_USER, GMAIL_PASSWORD)
            smtp.sendmail(GMAIL_USER, [NOTIFY_EMAIL], msg.as_string())
        log.info(f"Email enviado a {NOTIFY_EMAIL} ({len(new_pubs)} publicaciones)")
    except Exception as e:
        log.error(f"Error enviando email: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Monitor Diario Oficial DSP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python scraper.py                          Escaneo del día de hoy
  python scraper.py --historical             Escaneo desde 15-02-2025 hasta hoy
  python scraper.py --date 13-04-2026        Escaneo de una fecha específica
  python scraper.py --test                   Diagnóstico con edición conocida (13-04-2026)
  python scraper.py --test --date 07-04-2026 Diagnóstico de otra fecha
        """
    )
    parser.add_argument("--historical", action="store_true",
                        help=f"Escanear desde {START_DATE} hasta hoy")
    parser.add_argument("--date",       type=str,
                        help="Fecha específica DD-MM-YYYY")
    parser.add_argument("--test",       action="store_true",
                        help="Modo diagnóstico: verifica acceso a cada sección")
    parser.add_argument("--verbose",    action="store_true",
                        help="Log detallado de cada sección y keyword")
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    })

    keywords = load_keywords()

    # ── MODO DIAGNÓSTICO ──────────────────────────────────────────────────────
    if args.test:
        if args.date:
            target = date(int(args.date[6:10]), int(args.date[3:5]), int(args.date[:2]))
        else:
            target = date(2026, 4, 13)   # edición conocida y verificada
        run_diagnostic(session, target, keywords)
        return

    # ── MODOS NORMALES ────────────────────────────────────────────────────────
    data    = load_data()
    all_new = []
    verbose = args.verbose

    if args.historical:
        log.info("Limpiando estado previo para escaneo histórico limpio…")
        data["skipped_dates"]  = []
        data["editions_cache"] = {}
        log.info(f"=== ESCANEO HISTÓRICO: {START_DATE} → {date.today()} ===")
        current = START_DATE
        count   = 0
        while current <= date.today():
            if current.weekday() < 5:
                new = process_date(session, current, data, keywords, verbose=verbose)
                all_new.extend(new)
                count += 1
                if count % 10 == 0:
                    save_data(data)   # guardar progreso cada 10 días
            current += timedelta(days=1)

    elif args.date:
        d = date(int(args.date[6:10]), int(args.date[3:5]), int(args.date[:2]))
        log.info(f"=== FECHA ESPECÍFICA: {args.date} ===")
        all_new = process_date(session, d, data, keywords, verbose=verbose)

    else:
        today = date.today()
        log.info(f"=== ESCANEO DIARIO: {today} ===")
        if today.weekday() < 5:
            all_new = process_date(session, today, data, keywords, verbose=verbose)
        else:
            log.info("Fin de semana — sin edición del DO.")

    save_data(data)

    if all_new:
        send_email_alert(all_new)
        log.info(f"Total nuevas coincidencias encontradas: {len(all_new)}")
    else:
        log.info("Sin nuevas coincidencias en esta ejecución.")


if __name__ == "__main__":
    main()
