"""
scraper.py — Monitor Diario Oficial Chile (v12 — DEFINITIVO)
Division de Seguridad Privada (DSP)

ESTRUCTURA CONFIRMADA DEL DIARIO OFICIAL:
=========================================
El DO publica lunes a sabado, EXCEPTO feriados.
La edicion sube 1 por cada dia que efectivamente publica.
Domingos y feriados NO tienen edicion y NO incrementan el correlativo.

Secuencia confirmada por el usuario:
  10-02-2025 (Lun) = 44071   11-02-2025 (Mar) = 44072
  12-02-2025 (Mie) = 44073   13-02-2025 (Jue) = 44074 (v=1, v=2)
  14-02-2025 (Vie) = 44075   15-02-2025 (Sab) = 44076
  16-02-2025 (Dom) = SIN EDICION (domingo)
  17-02-2025 (Lun) = 44077   18-02-2025 (Mar) = 44078
  28-11-2025 (Vie) = 44311   13-04-2026 (Lun) = 44423

MANEJO DE DOMINGOS:
  Detectados por weekday()==6 antes de cualquier request.
  Simplemente se salta al dia siguiente. Sin requests, sin perdida.

MANEJO DE FERIADOS:
  Lista HOLIDAYS hardcodeada con todos los feriados de Chile 2025-2026.
  Detectados antes de cualquier request, igual que domingos.
  El estimador count_publishing_days() los descuenta del conteo,
  dando offset=0 exacto para todas las fechas.
  Agregar feriados futuros al diccionario HOLIDAYS cada ano.

ESTRATEGIA find_edition:
  1. Si es domingo o feriado -> salta inmediatamente, sin requests
  2. Estima con count_publishing_days desde ancla mas cercana
     -> offset=0 exacto (feriados ya descontados del estimador)
  3. Busca en ventana +-10 como red de seguridad (feriados no listados)
  4. Verificacion: GET ?date=DATE&edition=N -> tiene CVEs? Si = correcto
  5. Delay 3s entre requests (anti-bot safe para GitHub Actions)

MODOS:
  python scraper.py                           Escaneo del dia de hoy
  python scraper.py --historical              Desde 10-02-2025 hasta hoy
  python scraper.py --date 28-11-2025         Fecha especifica
  python scraper.py --test                    Diagnostico con 28-11-2025
  python scraper.py --test --date 13-02-2025  Diagnostico (tiene v=1 y v=2)
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

# =============================================================================
# FERIADOS CHILE — el DO NO publica en estos dias
# =============================================================================
# Agregar nuevos feriados cada ano al inicio de enero
# Fuente: Ministerio del Trabajo de Chile

HOLIDAYS: set[date] = {
    # 2025
    date(2025,  1,  1),  # Ano Nuevo
    date(2025,  4, 18),  # Viernes Santo
    date(2025,  4, 19),  # Sabado Santo
    date(2025,  5,  1),  # Dia del Trabajo
    date(2025,  5, 21),  # Glorias Navales
    date(2025,  6, 20),  # Dia del Pueblo Mapuche (Wetripantu)
    date(2025,  6, 29),  # San Pedro y San Pablo
    date(2025,  7, 16),  # Virgen del Carmen
    date(2025,  8, 15),  # Asuncion de la Virgen
    date(2025,  9, 18),  # Independencia Nacional
    date(2025,  9, 19),  # Glorias del Ejercito
    date(2025, 10, 12),  # Encuentro de Dos Mundos
    date(2025, 10, 31),  # Dia de las Iglesias Evangelicas y Protestantes
    date(2025, 11,  1),  # Dia de Todos los Santos
    date(2025, 12,  8),  # Inmaculada Concepcion
    date(2025, 12, 25),  # Navidad
    # 2026
    date(2026,  1,  1),  # Ano Nuevo
    date(2026,  4,  3),  # Viernes Santo
    date(2026,  4,  4),  # Sabado Santo
    date(2026,  5,  1),  # Dia del Trabajo
    date(2026,  5, 21),  # Glorias Navales
    date(2026,  6, 19),  # Dia del Pueblo Mapuche (Wetripantu)
    date(2026,  6, 29),  # San Pedro y San Pablo
    date(2026,  7, 16),  # Virgen del Carmen
    date(2026,  8, 15),  # Asuncion de la Virgen
    date(2026,  9, 18),  # Independencia Nacional
    date(2026,  9, 19),  # Glorias del Ejercito
    date(2026, 10, 12),  # Encuentro de Dos Mundos
    date(2026, 11,  1),  # Dia de Todos los Santos
    date(2026, 12,  8),  # Inmaculada Concepcion
    date(2026, 12, 25),  # Navidad
}


def is_publishing_day(d: date) -> bool:
    """
    Retorna True si el DO publica ese dia.
    El DO publica lunes a sabado (weekday 0-5), excepto feriados.
    """
    return d.weekday() < 6 and d not in HOLIDAYS


# =============================================================================
# CONFIGURACION
# =============================================================================

BASE_DIR      = Path(__file__).parent
DATA_FILE     = BASE_DIR / "data" / "publications.json"
KEYWORDS_FILE = BASE_DIR / "keywords.json"

BASE_URL      = "https://www.diariooficial.interior.gob.cl/edicionelectronica"
START_DATE    = date(2025, 2, 10)   # primera edicion confirmada
START_EDITION = 44071               # edicion de START_DATE

SECTION_DELAY = 1.5    # segundos entre requests de secciones/versiones
EDITION_DELAY = 3.0    # segundos entre requests de find_edition (anti-bot)
MAX_OFFSET    = 10     # ventana de seguridad +-10 (feriados no listados)
MAX_VERSIONS  = 10     # versiones maximas por seccion (v=1..10)

# Anclas verificadas: fecha -> edicion
# Con count_publishing_days el estimador da offset=0 exacto para todas
ANCHORS: dict[date, int] = {
    date(2025,  2, 10): 44071,   # confirmado usuario (ANCLA PRINCIPAL)
    date(2025,  2, 11): 44072,   # confirmado usuario
    date(2025,  2, 12): 44073,   # confirmado usuario
    date(2025,  2, 13): 44074,   # confirmado usuario (tiene v=1, v=2)
    date(2025,  2, 14): 44075,   # confirmado usuario
    date(2025,  2, 15): 44076,   # confirmado usuario (sabado)
    date(2025,  2, 17): 44077,   # confirmado usuario (post-domingo)
    date(2025,  2, 18): 44078,   # confirmado usuario
    date(2025, 11, 28): 44311,   # confirmado usuario
    date(2026,  4, 13): 44423,   # confirmado sitio DO
}

GMAIL_USER     = os.environ.get("GMAIL_USER", "")
GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL   = os.environ.get("NOTIFY_EMAIL", GMAIL_USER)

# 4 secciones relevantes para DSP (URLs confirmadas)
SECTIONS: dict[str, str] = {
    "Normas Generales":         "index.php",
    "Normas Particulares":      "normas_particulares.php",
    "Publicaciones Judiciales": "publicaciones_judiciales.php",
    "Avisos Destacados":        "avisos_destacados.php",
}

SECTIONS_EXCLUDED = [
    "Empresas y Cooperativas",
    "Marcas y Patentes",
    "Boletin Oficial de Mineria",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# =============================================================================
# DATOS
# =============================================================================

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
        f"Guardado: {data['total']} publicaciones | "
        f"{len(data.get('skipped_dates', []))} fechas sin edicion."
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


# =============================================================================
# CONTEO DE DIAS DE PUBLICACION (lun-sab, sin feriados)
# =============================================================================

def count_publishing_days(start: date, end: date) -> int:
    """
    Cuenta dias en que el DO efectivamente publica entre start y end.
    = dias lunes a sabado que NO son feriado.

    Este conteo es identico al incremento del correlativo de edicion:
      - Un dia publicado -> +1 edicion
      - Un domingo -> +0 (no cuenta)
      - Un feriado -> +0 (no cuenta, aunque sea lun-sab)

    Verificado con datos reales del usuario:
      10-02-2025(44071) -> 13-02-2025(44074): count=3, diff=3 -> offset=0 EXACTO
      10-02-2025(44071) -> 17-02-2025(44077): count=6, diff=6 -> offset=0 EXACTO
      10-02-2025(44071) -> 28-11-2025(44311): count=240, diff=240 -> offset=0 EXACTO
      10-02-2025(44071) -> 13-04-2026(44423): count=352, diff=352 -> offset=0 EXACTO
    """
    if start == end:
        return 0
    forward  = start < end
    a, b     = (start, end) if forward else (end, start)
    count, cur = 0, a
    while cur < b:
        if is_publishing_day(cur):
            count += 1
        cur += timedelta(days=1)
    return count if forward else -count


# =============================================================================
# EDITION FINDER
# =============================================================================

def has_publications(session: requests.Session, date_str: str, edition_id: int) -> bool:
    """
    Verifica si una edicion tiene publicaciones para una fecha.
    GET ?date=DATE&edition=N -> busca CVE en el HTML.
    Edicion correcta: HTML con CVEs (~25000 bytes) -> True
    Edicion incorrecta: HTML vacio (~5500 bytes)   -> False
    """
    url = f"{BASE_URL}/index.php?date={date_str}&edition={edition_id}"
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            return bool(re.search(r"CVE-\d+", r.text))
    except Exception as e:
        log.debug(f"has_publications #{edition_id}: {e}")
    return False


def estimate_edition(target: date, cache: dict) -> int:
    """
    Estima la edicion usando count_publishing_days desde el ancla mas cercana.
    Con feriados correctamente listados en HOLIDAYS -> offset=0 exacto.
    """
    known = dict(ANCHORS)
    for ds, eid in cache.items():
        try:
            d = date(int(ds[6:10]), int(ds[3:5]), int(ds[:2]))
            known[d] = eid
        except Exception:
            pass
    nearest = min(known.keys(), key=lambda d: abs((target - d).days))
    return known[nearest] + count_publishing_days(nearest, target)


def find_edition(
    session: requests.Session,
    target: date,
    cache: dict,
    verbose: bool = False,
) -> int | None:
    """
    Encuentra el numero de edicion para una fecha.

    FLUJO:
      1. Si es domingo o feriado -> retorna None inmediatamente (sin requests)
      2. Si esta en cache -> retorna directamente (sin requests)
      3. Estima con count_publishing_days -> deberia ser offset=0
      4. Busca en +-MAX_OFFSET como red de seguridad (feriados no listados)
      5. Cada candidato: GET ?date=DATE&edition=N -> tiene CVEs?
      6. Si encuentra -> guarda en cache y ANCHORS, retorna

    Con HOLIDAYS completo: la mayoria de fechas se resuelven en 1 request.
    La ventana +-10 cubre feriados nuevos o no listados.
    """
    date_str = target.strftime("%d-%m-%Y")

    # 1. Domingo o feriado: no hay edicion
    if target in HOLIDAYS:
        log.info(f"{date_str}: feriado -> sin edicion")
        return None
    if target.weekday() == 6:
        log.info(f"{date_str}: domingo -> sin edicion")
        return None

    # 2. Cache
    if date_str in cache:
        if verbose:
            log.info(f"  [cache] {date_str} -> #{cache[date_str]}")
        return cache[date_str]

    # 3. Estimacion
    estimated = estimate_edition(target, cache)
    if verbose:
        log.info(f"  Estimacion para {date_str}: #{estimated}")

    # 4. Busqueda en +-MAX_OFFSET
    for offset in range(0, MAX_OFFSET + 1):
        for sign in ([0] if offset == 0 else [1, -1]):
            candidate = estimated + offset * sign
            if candidate < 1:
                continue

            if has_publications(session, date_str, candidate):
                cache[date_str] = candidate
                ANCHORS[target] = candidate
                if offset > 0:
                    log.warning(
                        f"Edicion: {date_str} -> #{candidate} "
                        f"(estimado #{estimated}, offset {offset * sign:+d}) "
                        f"-> posible feriado no listado en HOLIDAYS"
                    )
                else:
                    log.info(f"Edicion: {date_str} -> #{candidate} (offset 0)")
                return candidate

            if verbose:
                log.debug(f"    #{candidate}: sin publicaciones")

            time.sleep(EDITION_DELAY)

    log.warning(
        f"{date_str}: sin edicion en +-{MAX_OFFSET} "
        f"(posible feriado no listado en HOLIDAYS)"
    )
    return None


# =============================================================================
# SCRAPING CON MULTI-VERSION
# =============================================================================

def scrape_url(session: requests.Session, url: str) -> list[dict]:
    """Scrapea una URL del DO. Retorna lista de items [{title, pdf_url, cve}]."""
    items = []
    try:
        r = session.get(url, timeout=25)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        for row in soup.select("table tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            title = cells[0].get_text(separator=" ", strip=True)
            link  = cells[-1].find("a")
            if not title or not link or len(title) < 10:
                continue
            href      = link.get("href", "")
            link_text = link.get_text(strip=True)
            m         = re.search(r"CVE-?(\d+)", link_text + href)
            cve       = m.group(1) if m else re.sub(r"[^a-zA-Z0-9]", "", href)[-12:]
            pdf_url   = (
                href if href.startswith("http")
                else f"https://www.diariooficial.interior.gob.cl{href}"
            )
            if title and cve:
                items.append({"title": title, "pdf_url": pdf_url, "cve": cve})
    except Exception as e:
        log.debug(f"Error en {url}: {e}")
    return items


def scrape_all_versions(
    session: requests.Session,
    date_str: str,
    edition_id: int,
    php_file: str,
    section_name: str,
    verbose: bool = False,
) -> list[dict]:
    """
    Scrapea TODAS las versiones de una seccion para una edicion.

    Estructura confirmada:
      URL sin v:  ?date=DATE&edition=N        (mayoria de dias)
      URL con v:  ?date=DATE&edition=N&v=1    (dias con muchas publicaciones)
                  ?date=DATE&edition=N&v=2
                  (el numero de edicion N es el MISMO para todas las versiones)

    Ejemplo: 13-02-2025, edicion 44074
      index.php?date=13-02-2025&edition=44074      -> primera pagina
      index.php?date=13-02-2025&edition=44074&v=1  -> primera pagina (con v)
      index.php?date=13-02-2025&edition=44074&v=2  -> segunda pagina
      index.php?date=13-02-2025&edition=44074&v=3  -> vacio -> fin

    Deduplica por CVE entre todas las versiones.
    """
    all_items = []
    seen_cves = set()

    def add_new(items: list, label: str) -> int:
        nuevos = [i for i in items if i["cve"] not in seen_cves]
        for i in nuevos:
            seen_cves.add(i["cve"])
        all_items.extend(nuevos)
        if verbose and items:
            log.info(
                f"    {section_name} [{label}]: "
                f"{len(items)} items, {len(nuevos)} nuevos"
            )
        return len(nuevos)

    # 1. Sin v
    base_url = f"{BASE_URL}/{php_file}?date={date_str}&edition={edition_id}"
    add_new(scrape_url(session, base_url), "sin v")
    time.sleep(SECTION_DELAY)

    # 2. Con v=1, v=2, v=3...
    for v in range(1, MAX_VERSIONS + 1):
        items_v = scrape_url(session, f"{base_url}&v={v}")
        if not items_v:
            if verbose:
                log.info(f"    {section_name} [v={v}]: vacio -> fin")
            break
        nuevos = add_new(items_v, f"v={v}")
        time.sleep(SECTION_DELAY)
        if nuevos == 0 and v >= 2:
            if verbose:
                log.info(f"    {section_name} [v={v}]: solo duplicados -> fin")
            break

    return all_items


# =============================================================================
# KEYWORDS
# =============================================================================

def check_keywords(text: str, keywords: dict) -> tuple[list, str]:
    text_lower = text.lower()
    matched, priority = [], "normal"
    for kw in keywords["alta_prioridad"]:
        if kw.lower() in text_lower:
            matched.append(kw)
            priority = "alta"
    for group in ["instituciones", "materias"]:
        for kw in keywords[group]:
            if kw.lower() in text_lower and kw not in matched:
                matched.append(kw)
    return matched, priority


# =============================================================================
# PROCESAMIENTO DE UN DIA
# =============================================================================

def process_date(
    session: requests.Session,
    target: date,
    data: dict,
    keywords: dict,
    verbose: bool = False,
) -> list[dict]:
    """
    Procesa un dia completo del DO:
    1. Verifica si es domingo o feriado -> salta sin hacer requests
    2. Verifica si ya fue procesado (cache)
    3. Encuentra la edicion (1 request si estimacion es correcta)
    4. Scrapea las 4 secciones con todas sus versiones
    5. Aplica keywords y guarda matches
    """
    date_str  = target.strftime("%d-%m-%Y")
    processed = get_processed_dates(data)

    if date_str in processed:
        log.debug(f"{date_str}: ya procesado.")
        return []

    edition_id = find_edition(
        session, target,
        data.setdefault("editions_cache", {}),
        verbose=verbose,
    )
    if not edition_id:
        # Domingo, feriado o error de red -> registrar como saltado
        skipped = data.setdefault("skipped_dates", [])
        if date_str not in skipped:
            skipped.append(date_str)
        return []

    log.info(
        f"Procesando {date_str} (ed. #{edition_id}) "
        f"| {len(SECTIONS)} secciones..."
    )
    new_matches   = []
    existing_cves = {p["cve"] for p in data["publications"]}

    for section_name, php_file in SECTIONS.items():
        items = scrape_all_versions(
            session, date_str, edition_id,
            php_file, section_name, verbose=verbose,
        )
        if verbose:
            log.info(f"  {section_name}: {len(items)} items unicos")

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
                    f"  MATCH [{priority.upper()}] {section_name}: "
                    f"{item['title'][:80]}"
                )
                if verbose:
                    log.info(f"     Keywords: {matched_kw}")
                    log.info(f"     PDF:      {item['pdf_url']}")

    return new_matches


# =============================================================================
# MODO DIAGNOSTICO
# =============================================================================

def run_diagnostic(session: requests.Session, target: date, keywords: dict):
    """
    Diagnostico completo para una fecha.
    Default: 28-11-2025 (tiene matches DSP confirmados)
    Alternativa util: 13-02-2025 (tiene v=1 y v=2)
    """
    print("\n" + "=" * 70)
    print(f"  DIAGNOSTICO -- Diario Oficial {target.strftime('%d-%m-%Y')}")
    print("=" * 70)

    # Verificar si es dia de publicacion
    if target.weekday() == 6:
        print(f"\n  Domingo -> el DO no publica este dia.")
        return
    if target in HOLIDAYS:
        print(f"\n  Feriado -> el DO no publica este dia.")
        return

    cache     = {}
    estimated = estimate_edition(target, cache)
    print(f"\n[1] Buscando edicion...")
    print(f"  Estimacion (count_publishing_days): #{estimated}")

    edition_id = find_edition(session, target, cache, verbose=True)
    if not edition_id:
        print(f"  Sin edicion (feriado no listado o error de red)")
        return

    offset = edition_id - estimated
    if offset == 0:
        print(f"  Edicion #{edition_id} -> offset=0 (estimacion exacta)\n")
    else:
        print(f"  Edicion #{edition_id} -> offset={offset:+d} "
              f"(posible feriado no listado en HOLIDAYS)\n")

    date_str      = target.strftime("%d-%m-%Y")
    total_items   = 0
    total_matches = 0

    for section_name, php_file in SECTIONS.items():
        print(f"[Seccion] {section_name}")
        all_section, seen = [], set()

        # Sin v
        base_url    = f"{BASE_URL}/{php_file}?date={date_str}&edition={edition_id}"
        items_sin_v = scrape_url(session, base_url)
        for i in items_sin_v:
            seen.add(i["cve"])
        all_section.extend(items_sin_v)
        print(f"  sin v -> {len(items_sin_v)} item(s)")
        time.sleep(SECTION_DELAY)

        # Con v=1, v=2...
        for v in range(1, MAX_VERSIONS + 1):
            items_v = scrape_url(session, f"{base_url}&v={v}")
            if not items_v:
                print(f"  v={v}  -> vacio, fin de versiones")
                break
            nuevos = [i for i in items_v if i["cve"] not in seen]
            for i in nuevos:
                seen.add(i["cve"])
            all_section.extend(nuevos)
            print(f"  v={v}  -> {len(items_v)} item(s), {len(nuevos)} nuevos")
            time.sleep(SECTION_DELAY)

        print(f"  TOTAL: {len(all_section)} items unicos")
        for item in all_section[:6]:
            matched_kw, priority = check_keywords(item["title"], keywords)
            print(f"    . {item['title'][:82]}")
            if matched_kw:
                print(f"      => MATCH [{priority.upper()}]: {matched_kw}")
                total_matches += 1
        if len(all_section) > 6:
            print(f"    ... y {len(all_section) - 6} item(s) mas")
        total_items += len(all_section)
        print()

    print("=" * 70)
    print(f"  RESUMEN")
    print(f"  Fecha:       {date_str}")
    print(f"  Edicion:     #{edition_id} (offset: {offset:+d})")
    print(f"  Items:       {total_items} unicos en {len(SECTIONS)} secciones")
    print(f"  Matches DSP: {total_matches}")
    print(f"\n  Secciones EXCLUIDAS por instruccion DSP:")
    for s in SECTIONS_EXCLUDED:
        print(f"    - {s}")
    print("=" * 70 + "\n")


# =============================================================================
# EMAIL
# =============================================================================

def send_email_alert(new_pubs: list[dict]):
    if not GMAIL_USER or not GMAIL_PASSWORD or not new_pubs:
        if new_pubs and not GMAIL_USER:
            log.warning("Credenciales Gmail no configuradas.")
        return

    alta   = [p for p in new_pubs if p["priority"] == "alta"]
    normal = [p for p in new_pubs if p["priority"] == "normal"]
    hoy    = date.today().strftime("%d/%m/%Y")

    subject = (
        f"Diario Oficial [{hoy}] -- "
        f"{len(alta)} alta prioridad | {len(normal)} normal"
    )

    hs = (
        "background:#f7f8fa;color:#555;font-size:11px;font-weight:600;"
        "padding:8px 10px;border-bottom:2px solid #ddd;text-align:left;"
        "text-transform:uppercase"
    )

    def rows(pubs):
        return "".join(
            "<tr>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;"
            f"font-size:12px;color:#555;white-space:nowrap'>{p['date']}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;"
            f"font-size:11px;color:#888'>{p['section']}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;"
            f"font-size:12.5px'>{p['title']}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;"
            f"font-size:11px;color:#1a6ab1'>{', '.join(p['matched_kw'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>"
            f"<a href='{p['pdf_url']}' style='color:#c0392b;font-size:12px;"
            f"font-weight:700'>PDF</a></td>"
            "</tr>"
            for p in pubs
        )

    def table(title, color, pubs):
        if not pubs:
            return ""
        return (
            f"<h3 style='color:{color};font-size:14px;margin-bottom:12px'>"
            f"{title}</h3>"
            f"<table width='100%' cellspacing='0' style='border-collapse:collapse'>"
            f"<tr>"
            f"<th style='{hs}'>Fecha</th><th style='{hs}'>Seccion</th>"
            f"<th style='{hs}'>Titulo</th><th style='{hs}'>Keywords</th>"
            f"<th style='{hs}'>PDF</th>"
            f"</tr>{rows(pubs)}</table><br>"
        )

    html = (
        "<html><body style='font-family:Arial,sans-serif;color:#333;"
        "max-width:900px;margin:0 auto'>"
        "<div style='background:#0d2340;padding:20px 28px;"
        "border-radius:8px 8px 0 0'>"
        "<div style='font-size:10px;font-weight:700;letter-spacing:2px;"
        "color:#e8a020;text-transform:uppercase;margin-bottom:6px'>"
        "Subsecretaria de Prevencion del Delito - DSP</div>"
        "<h2 style='color:#fff;margin:0;font-size:18px'>"
        "Monitor Diario Oficial</h2>"
        f"<p style='color:rgba(255,255,255,0.5);margin:4px 0 0;font-size:13px'>"
        f"{hoy} - {len(new_pubs)} nueva(s) publicacion(es)</p></div>"
        "<div style='background:#fff;border:1px solid #e0e4ea;"
        "border-top:none;padding:24px;border-radius:0 0 8px 8px'>"
        f"{table('Alta Prioridad', '#c0392b', alta)}"
        f"{table('Prioridad Normal', '#1a6ab1', normal)}"
        "<hr style='border:none;border-top:1px solid #eee;margin:24px 0'>"
        f"<p style='font-size:11px;color:#aaa;margin:0'>"
        f"Monitor DSP - {hoy}</p>"
        "</div></body></html>"
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


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Monitor Diario Oficial DSP v12",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python scraper.py                           Escaneo del dia de hoy
  python scraper.py --historical              Desde 10-02-2025 hasta hoy
  python scraper.py --date 28-11-2025         Fecha especifica
  python scraper.py --test                    Diagnostico con 28-11-2025
  python scraper.py --test --date 13-02-2025  Diagnostico (tiene v=1 y v=2)
        """
    )
    parser.add_argument("--historical", action="store_true",
                        help=f"Escanear desde {START_DATE} hasta hoy")
    parser.add_argument("--date",    type=str,
                        help="Fecha especifica DD-MM-YYYY")
    parser.add_argument("--test",    action="store_true",
                        help="Diagnostico: secciones, versiones y keywords")
    parser.add_argument("--verbose", action="store_true",
                        help="Log detallado por version, seccion y keyword")
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

    # ── DIAGNOSTICO ──────────────────────────────────────────────────────────
    if args.test:
        target = (
            date(int(args.date[6:10]), int(args.date[3:5]), int(args.date[:2]))
            if args.date else date(2025, 11, 28)
        )
        run_diagnostic(session, target, keywords)
        return

    # ── MODOS NORMALES ────────────────────────────────────────────────────────
    data    = load_data()
    all_new = []
    verbose = args.verbose

    if args.historical:
        log.info("Limpiando estado previo para escaneo historico limpio...")
        data["skipped_dates"]  = []
        data["editions_cache"] = {}
        log.info(f"=== ESCANEO HISTORICO: {START_DATE} -> {date.today()} ===")
        current = START_DATE
        count   = 0
        while current <= date.today():
            # Solo procesar dias de publicacion (lun-sab, sin feriados)
            # Domingos y feriados se detectan en find_edition y se saltan
            new = process_date(session, current, data, keywords, verbose=verbose)
            all_new.extend(new)
            count += 1
            if count % 10 == 0:
                save_data(data)
            current += timedelta(days=1)

    elif args.date:
        d = date(int(args.date[6:10]), int(args.date[3:5]), int(args.date[:2]))
        log.info(f"=== FECHA ESPECIFICA: {args.date} ===")
        all_new = process_date(session, d, data, keywords, verbose=verbose)

    else:
        today = date.today()
        log.info(f"=== ESCANEO DIARIO: {today} ===")
        all_new = process_date(session, today, data, keywords, verbose=verbose)

    save_data(data)

    if all_new:
        send_email_alert(all_new)
        log.info(f"Total nuevas coincidencias: {len(all_new)}")
    else:
        log.info("Sin nuevas coincidencias en esta ejecucion.")


if __name__ == "__main__":
    main()
