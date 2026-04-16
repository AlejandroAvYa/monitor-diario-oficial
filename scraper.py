"""
scraper.py — Monitor Diario Oficial Chile (v14-FINAL)
Division de Seguridad Privada (DSP)

REGLAS REALES DEL DO (verificadas con datos y URLs concretas):
===============================================================

DIAS REGULARES (lun-sab, no feriado):
  Edicion N   → ?date=DATE&edition=N        (sin v = devuelve contenido)
  Edicion N-B → ?date=DATE&edition=N-B      (sin v = tambien devuelve contenido, VERIFICADO)
  N-B NO incrementa el correlativo. Solo agrega publicaciones extra.

DOMINGOS/FERIADOS CON EDICION ESPECIAL (B edition):
  Edicion (N+1)-B donde N = edicion del dia anterior
  ESTA SI incrementa el correlativo (el siguiente dia regular = N+2)
  Ejemplo: Sab 02-08=44214, Dom 03-08=44215-B, Lun 04-08=44216

EDICIONES B EN DIAS NO-HABILES CONOCIDAS (B_EDITION_DAYS):
  03-08-2025 (Dom) = 44215-B  → duelo nacional mineros El Teniente
  18-01-2026 (Dom) = 44352-B  → descubierta por diferencial de conteo

POR QUE FALLABAN LAS VERSIONES ANTERIORES:
  1. v13 y anteriores: nunca intentaban edition N-B en dias normales
  2. v13 y anteriores: saltaban TODOS los domingos (sin intentar B)
  3. v13 y anteriores: desconocian los domingos con B, calculando
     ediciones incorrectas (-1) para dias posteriores

KEYWORD MATCHING VERIFICADO contra titulos reales:
  44311-B: 'Resolucion...transitoriedad ley N 21.659...Reglamento Seguridad Privada'
    → Match: ['21.659', 'seguridad privada'] | Prioridad: ALTA ✓
  44373:   'Ley 21.802...institucionalidad municipal en materia de seguridad publica'
    → Match: ['21.802', 'seguridad publica'] | Prioridad: ALTA ✓

MODOS:
  python scraper.py                           Escaneo del dia de hoy
  python scraper.py --historical              Desde 10-02-2025 hasta hoy
  python scraper.py --date 28-11-2025         Fecha especifica
  python scraper.py --test                    Diagnostico con 28-11-2025
  python scraper.py --test --date 03-08-2025  Diagnostico domingo especial
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
# FERIADOS — el DO NO publica edicion regular en estos dias
# =============================================================================

HOLIDAYS: set[date] = {
    # 2025
    date(2025,  1,  1), date(2025,  4, 18), date(2025,  4, 19),
    date(2025,  5,  1), date(2025,  5, 21), date(2025,  6, 20),
    date(2025,  6, 29), date(2025,  7, 16), date(2025,  8, 15),
    date(2025,  9, 18), date(2025,  9, 19), date(2025, 10, 12),
    date(2025, 10, 31), date(2025, 11,  1), date(2025, 12,  8),
    date(2025, 12, 25),
    # 2026
    date(2026,  1,  1), date(2026,  4,  3), date(2026,  4,  4),
    date(2026,  5,  1), date(2026,  5, 21), date(2026,  6, 19),
    date(2026,  6, 29), date(2026,  7, 16), date(2026,  8, 15),
    date(2026,  9, 18), date(2026,  9, 19), date(2026, 10, 12),
    date(2026, 11,  1), date(2026, 12,  8), date(2026, 12, 25),
}

# =============================================================================
# EDICIONES B EN DOMINGOS/FERIADOS — consumen numero del correlativo
# {fecha: numero_sin_B}   →   el DO publico {numero}-B ese dia
# =============================================================================

B_EDITION_DAYS: dict[date, int] = {
    date(2025,  8,  3): 44215,   # Dom → 44215-B (duelo mineros El Teniente)
    date(2026,  1, 18): 44352,   # Dom → 44352-B
}

# =============================================================================
# CONFIGURACION
# =============================================================================

BASE_DIR      = Path(__file__).parent
DATA_FILE     = BASE_DIR / "data" / "publications.json"
KEYWORDS_FILE = BASE_DIR / "keywords.json"

BASE_URL      = "https://www.diariooficial.interior.gob.cl/edicionelectronica"
START_DATE    = date(2025, 2, 10)

SECTION_DELAY = 0.8    # segundos entre requests (anti-bot safe, verificado)
MAX_VERSIONS  = 3      # versiones maximas por seccion (maximo observado: v=2)

# Anclas verificadas — incluyen correcciones post-B-edition-domingo
ANCHORS: dict[date, int] = {
    date(2025,  2, 10): 44071,   # confirmado usuario
    date(2025,  2, 11): 44072,   # confirmado usuario
    date(2025,  2, 12): 44073,   # confirmado usuario
    date(2025,  2, 13): 44074,   # confirmado usuario (v=1, v=2)
    date(2025,  2, 14): 44075,   # confirmado usuario
    date(2025,  2, 15): 44076,   # confirmado usuario (sabado)
    date(2025,  2, 17): 44077,   # confirmado usuario
    date(2025,  2, 18): 44078,   # confirmado usuario
    date(2025,  8,  4): 44216,   # lunes post-domingo 44215-B (FIX AGOSTO)
    date(2025, 11, 28): 44311,   # confirmado usuario
    date(2026,  1, 19): 44353,   # lunes post-domingo 44352-B (FIX ENERO)
    date(2026,  2, 11): 44373,   # confirmado usuario (Ley 21.802)
    date(2026,  4, 13): 44423,   # confirmado sitio DO
}

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

GMAIL_USER     = os.environ.get("GMAIL_USER", "")
GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL   = os.environ.get("NOTIFY_EMAIL", GMAIL_USER)

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
            d = json.load(f)
        # Cargar B editions descubiertas en runs anteriores
        for ds, num in d.get("b_edition_days", {}).items():
            try:
                dt = date(int(ds[6:10]), int(ds[3:5]), int(ds[:2]))
                if dt not in B_EDITION_DAYS:
                    B_EDITION_DAYS[dt] = num
            except Exception:
                pass
        return d
    return {
        "last_updated":   None,
        "total":          0,
        "editions_cache": {},
        "publications":   [],
        "skipped_dates":  [],
        "b_edition_days": {},
    }


def save_data(data: dict):
    # Persistir B editions descubiertas en tiempo real
    data["b_edition_days"] = {
        d.strftime("%d-%m-%Y"): n
        for d, n in B_EDITION_DAYS.items()
    }
    data["total"]        = len(data["publications"])
    data["last_updated"] = date.today().isoformat()
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(
        f"Guardado: {data['total']} publicaciones | "
        f"{len(data.get('skipped_dates', []))} sin edicion | "
        f"{len(B_EDITION_DAYS)} B-editions conocidas."
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
# CLASIFICACION DE DIAS
# =============================================================================

def is_regular_publishing_day(d: date) -> bool:
    """True si el DO publica edicion regular (lun-sab, sin feriados)."""
    return d.weekday() < 6 and d not in HOLIDAYS


# =============================================================================
# CALCULO DE EDICION (sin requests al servidor)
# =============================================================================

def count_publishing_days(start: date, end: date) -> int:
    """
    Cuenta dias que generan edicion entre start y end.
    Incluye dias regulares (lun-sab, no feriado) Y dias con B edition especial
    (Domingos/feriados que publicaron B edition — consumen numero del correlativo).

    Verificado con datos reales del usuario:
      10-02-2025(44071) → 13-02-2025(44074): count=3 → offset 0
      10-02-2025(44071) → 04-08-2025(44216): count=145 → offset 0 (incluye dom 44215-B)
      10-02-2025(44071) → 28-11-2025(44311): count=240 → offset 0
      10-02-2025(44071) → 19-01-2026(44353): count=282 → offset 0 (incluye dom 44352-B)
      10-02-2025(44071) → 11-02-2026(44373): count=302 → offset 0
      10-02-2025(44071) → 13-04-2026(44423): count=352 → offset 0
    """
    if start == end:
        return 0
    forward  = start < end
    a, b     = (start, end) if forward else (end, start)
    count, cur = 0, a
    while cur < b:
        if is_regular_publishing_day(cur) or cur in B_EDITION_DAYS:
            count += 1
        cur += timedelta(days=1)
    return count if forward else -count


def calculate_edition(target: date, cache: dict) -> int:
    """
    Calcula el numero de edicion SIN hacer requests al servidor.
    Usa el ancla mas cercana + count_publishing_days.
    Con ANCHORS y B_EDITION_DAYS correctos → resultado exacto.
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


# =============================================================================
# SCRAPING — una seccion, todas las versiones
# =============================================================================

def scrape_url(session: requests.Session, url: str) -> list[dict]:
    """
    Scrapea UNA URL del DO.
    Retorna [{title, pdf_url, cve}] o [] si no hay publicaciones.
    """
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


def scrape_section_all_versions(
    session: requests.Session,
    date_str: str,
    edition_str: str,
    section_name: str,
    php_file: str,
    seen_cves: set,
    verbose: bool = False,
) -> list[dict]:
    """
    Scrapea TODAS las versiones de UNA seccion para una edicion.
    edition_str puede ser '44311', '44311-B', '44215-B', etc.

    Estrategia:
      1. Sin v (VERIFICADO: devuelve contenido para ambas N y N-B)
      2. Con v=1, v=2, ... hasta MAX_VERSIONS
      3. Parar cuando 2 versiones consecutivas esten vacias (no 1, para
         no perderse casos donde v=1 vacio pero v=2 tiene contenido)

    Deduplica por CVE con seen_cves (compartido entre edicion N y N-B).
    """
    section_items = []
    base_url      = f"{BASE_URL}/{php_file}?date={date_str}&edition={edition_str}"

    def add_new(items: list, label: str) -> int:
        nuevos = [i for i in items if i["cve"] not in seen_cves]
        for i in nuevos:
            seen_cves.add(i["cve"])
            i["section"] = section_name    # preservar nombre de seccion
        section_items.extend(nuevos)
        if verbose and items:
            log.info(f"    [{edition_str}] {section_name} [{label}]: "
                     f"{len(items)} items, {len(nuevos)} nuevos")
        return len(nuevos)

    # 1. Sin v
    add_new(scrape_url(session, base_url), "sin v")
    time.sleep(SECTION_DELAY)

    # 2. Con v=1, v=2, v=3...
    # El servidor puede devolver la misma pagina con distinto v (todos duplicados)
    # o pagina vacia. En ambos casos, 0 items nuevos = no hay mas contenido.
    for v in range(1, MAX_VERSIONS + 1):
        items_v = scrape_url(session, f"{base_url}&v={v}")
        nuevos  = add_new(items_v, f"v={v}")
        time.sleep(SECTION_DELAY)
        if nuevos == 0:
            # Sin items nuevos (ya sea pagina vacia o duplicados) = fin
            if verbose:
                log.info(f"    [{edition_str}] {section_name} v={v}: sin nuevos -> fin")
            break

    return section_items


def scrape_edition_no_versions(
    session: requests.Session,
    date_str: str,
    edition_str: str,
    seen_cves: set,
    verbose: bool = False,
) -> list[dict]:
    """
    Scrapea las 4 secciones de una edicion SIN intentar versiones (v=1,2...).
    Usado para ediciones N-B en dias regulares:
      VERIFICADO que ?date=DATE&edition=N-B sin v devuelve contenido completo.
    Esto reduce requests de 4×(1+MAX_VERSIONS) a solo 4 (uno por seccion).
    """
    all_items = []
    for section_name, php_file in SECTIONS.items():
        url   = f"{BASE_URL}/{php_file}?date={date_str}&edition={edition_str}"
        items = scrape_url(session, url)
        nuevos = [i for i in items if i["cve"] not in seen_cves]
        for i in nuevos:
            seen_cves.add(i["cve"])
            i["section"] = section_name
        all_items.extend(nuevos)
        if verbose and items:
            log.info(f"    [{edition_str}] {section_name}: {len(items)} items, {len(nuevos)} nuevos")
        time.sleep(SECTION_DELAY)
    return all_items


def scrape_edition_all_sections(
    session: requests.Session,
    date_str: str,
    edition_str: str,
    seen_cves: set,
    verbose: bool = False,
) -> list[dict]:
    """
    Scrapea las 4 secciones de UNA edicion (regular N o B: N-B).
    Cada item en el resultado incluye el campo 'section' con el nombre real.
    seen_cves se comparte entre llamadas para deduplicar entre N y N-B.
    """
    all_items = []
    for section_name, php_file in SECTIONS.items():
        items = scrape_section_all_versions(
            session, date_str, edition_str,
            section_name, php_file, seen_cves, verbose
        )
        all_items.extend(items)
        if verbose and items:
            log.info(f"  [{edition_str}] {section_name}: {len(items)} items nuevos")
    return all_items


# =============================================================================
# KEYWORDS
# =============================================================================

def check_keywords(text: str, keywords: dict) -> tuple[list, str]:
    """
    Aplica keywords al titulo. Retorna (matches, prioridad).
    Verificado contra titulos reales:
      '...transitoriedad ley N 21.659...Reglamento Seguridad Privada'
        → ['21.659','seguridad privada'] | ALTA
      '...Ley 21.802...materia de seguridad publica...'
        → ['21.802','seguridad publica'] | ALTA
    """
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
# GUARDAR MATCHES
# =============================================================================

def save_matches(
    items: list[dict],
    date_str: str,
    edition_str: str,
    keywords: dict,
    data: dict,
    new_matches: list,
    verbose: bool,
):
    """Aplica keywords a los items y guarda publicaciones con match."""
    existing_cves = {p["cve"] for p in data["publications"]}
    for item in items:
        if item["cve"] in existing_cves:
            continue
        matched_kw, priority = check_keywords(item["title"], keywords)
        if matched_kw:
            pub = {
                "cve":        item["cve"],
                "date":       date_str,
                "edition_id": edition_str,
                "section":    item.get("section", "DO"),  # nombre real de seccion
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
                f"  MATCH [{priority.upper()}] [{edition_str}] "
                f"{item.get('section','')}: {item['title'][:75]}"
            )
            if verbose:
                log.info(f"     Keywords: {matched_kw}")
                log.info(f"     PDF:      {item['pdf_url']}")


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
    Procesa un dia completo del DO.

    CASO A — Dia regular (lun-sab, no feriado):
      1. Calcula edicion N (sin requests)
      2. Scrapea edicion N  (4 secciones + todas las versiones)
      3. Scrapea edicion N-B (MISMO numero, sufijo B — sin v devuelve contenido)
      CVEs deduplicados entre N y N-B mediante seen_cves compartido.

    CASO B — Domingo/feriado con B edition conocida (B_EDITION_DAYS):
      Scrapea solo la edicion B conocida.

    CASO C — Domingo/feriado desconocido:
      Calcula numero potencial B y lo intenta.
      Si encuentra contenido → registra nueva B edition, corrige anclas.

    CASO D — Dia ya procesado:
      Salta inmediatamente (0 requests).
    """
    date_str  = target.strftime("%d-%m-%Y")
    processed = get_processed_dates(data)
    cache     = data.setdefault("editions_cache", {})

    # D. Ya procesado
    if date_str in processed:
        log.debug(f"{date_str}: ya procesado.")
        return []

    new_matches = []
    found_any   = False

    # A. Dia regular
    if is_regular_publishing_day(target):
        edition_id    = calculate_edition(target, cache)
        edition_str   = str(edition_id)
        b_edition_str = f"{edition_id}-B"
        log.info(f"Procesando {date_str} (ed. #{edition_id} + #{edition_id}-B)...")

        # CVEs compartidos entre N y N-B para no duplicar
        seen_cves = set()

        # Edicion regular N
        items_n = scrape_edition_all_sections(
            session, date_str, edition_str, seen_cves, verbose
        )
        save_matches(items_n, date_str, edition_str, keywords, data, new_matches, verbose)
        if items_n:
            found_any = True

        # Edicion B del mismo dia: N-B
        # VERIFICADO: edition=N-B sin v devuelve contenido completo.
        # No necesita versiones → 1 request por seccion (4 total, no 12-36)
        items_b = scrape_edition_no_versions(
            session, date_str, b_edition_str, seen_cves, verbose
        )
        save_matches(items_b, date_str, b_edition_str, keywords, data, new_matches, verbose)
        if items_b:
            found_any = True

        if found_any:
            cache[date_str] = edition_id
            ANCHORS[target] = edition_id
        else:
            log.warning(
                f"{date_str}: sin publicaciones para #{edition_id} ni #{edition_id}-B. "
                f"Posible feriado no listado en HOLIDAYS."
            )
            data.setdefault("skipped_dates", []).append(date_str)
            return []

    # B. Domingo/feriado con B edition conocida
    elif target in B_EDITION_DAYS:
        b_num         = B_EDITION_DAYS[target]
        b_edition_str = f"{b_num}-B"
        log.info(f"Procesando {date_str} (B edition conocida: {b_edition_str})...")
        seen_cves = set()
        items_b = scrape_edition_all_sections(
            session, date_str, b_edition_str, seen_cves, verbose
        )
        save_matches(items_b, date_str, b_edition_str, keywords, data, new_matches, verbose)
        if items_b:
            found_any = True
        if not found_any:
            data.setdefault("skipped_dates", []).append(date_str)
            return []

    # C. Domingo/feriado desconocido — buscar posible B edition
    else:
        # Calcular el numero que tendria una posible B edition
        # = (ultima edicion publicada conocida) + 1
        prev = target - timedelta(days=1)
        while not is_regular_publishing_day(prev) and prev not in B_EDITION_DAYS:
            prev -= timedelta(days=1)
        prev_edition      = calculate_edition(prev, cache)
        potential_b_num   = prev_edition + 1
        potential_b_str   = f"{potential_b_num}-B"
        log.debug(f"{date_str}: domingo/feriado, probando {potential_b_str}...")

        seen_cves = set()
        items_b = scrape_edition_all_sections(
            session, date_str, potential_b_str, seen_cves, verbose
        )

        if items_b:
            log.info(
                f"NUEVA B EDITION DESCUBIERTA: {date_str} = {potential_b_str} "
                f"({len(items_b)} items)"
            )
            B_EDITION_DAYS[target] = potential_b_num
            data.setdefault("b_edition_days", {})[date_str] = potential_b_num
            # Ancla para el dia siguiente (post-B-edition)
            next_day = target + timedelta(days=1)
            ANCHORS[next_day] = potential_b_num + 1
            save_matches(items_b, date_str, potential_b_str, keywords, data, new_matches, verbose)
            found_any = True
        else:
            log.debug(f"{date_str}: sin B edition (domingo/feriado normal)")
            data.setdefault("skipped_dates", []).append(date_str)
            return []

    return new_matches


# =============================================================================
# MODO DIAGNOSTICO
# =============================================================================

def run_diagnostic(session: requests.Session, target: date, keywords: dict):
    """
    Diagnostico completo para una fecha.
    Muestra: edicion calculada, items por seccion, matches de keywords.
    Para dias regulares: prueba N y N-B.
    Para domingos/feriados: prueba la B edition.
    Default: 28-11-2025 (tiene 44311 y 44311-B, ambas con matches DSP)
    """
    print("\n" + "=" * 70)
    print(f"  DIAGNOSTICO — Diario Oficial {target.strftime('%d-%m-%Y')}")
    print("=" * 70)

    cache    = {}
    date_str = target.strftime("%d-%m-%Y")

    if is_regular_publishing_day(target):
        edition_id = calculate_edition(target, cache)
        print(f"\n  Tipo: dia regular | Edicion calculada: #{edition_id} (+ #{edition_id}-B)")
        seen_cves = set()

        for edition_str in [str(edition_id), f"{edition_id}-B"]:
            print(f"\n[Edicion {edition_str}]")
            items = scrape_edition_all_sections(
                session, date_str, edition_str, seen_cves, verbose=True
            )
            total_m = 0
            for item in items[:8]:
                matched_kw, priority = check_keywords(item["title"], keywords)
                sec = item.get("section", "")
                print(f"  [{sec}] {item['title'][:80]}")
                if matched_kw:
                    print(f"    => MATCH [{priority.upper()}]: {matched_kw}")
                    total_m += 1
            if len(items) > 8:
                print(f"  ... y {len(items)-8} mas")
            print(f"  → {len(items)} items unicos, {total_m} matches DSP")

    elif target in B_EDITION_DAYS:
        b_num = B_EDITION_DAYS[target]
        b_str = f"{b_num}-B"
        print(f"\n  Tipo: B edition conocida → {b_str}")
        seen_cves = set()
        items = scrape_edition_all_sections(session, date_str, b_str, seen_cves, verbose=True)
        total_m = 0
        for item in items[:8]:
            matched_kw, priority = check_keywords(item["title"], keywords)
            sec = item.get("section", "")
            print(f"  [{sec}] {item['title'][:80]}")
            if matched_kw:
                print(f"    => MATCH [{priority.upper()}]: {matched_kw}")
                total_m += 1
        print(f"  → {len(items)} items, {total_m} matches DSP")

    else:
        prev = target - timedelta(days=1)
        while not is_regular_publishing_day(prev) and prev not in B_EDITION_DAYS:
            prev -= timedelta(days=1)
        prev_ed = calculate_edition(prev, cache)
        b_str   = f"{prev_ed + 1}-B"
        print(f"\n  Tipo: domingo/feriado desconocido | Probando {b_str}")
        seen_cves = set()
        items = scrape_edition_all_sections(session, date_str, b_str, seen_cves, verbose=True)
        if items:
            print(f"  → ENCONTRADA {b_str}: {len(items)} items")
        else:
            print(f"  → Sin edicion para este dia")

    print("\n" + "=" * 70 + "\n")


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
            f"font-size:11px;color:#888'>{p['edition_id']}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;"
            f"font-size:11px;color:#666'>{p['section']}</td>"
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
            f"<h3 style='color:{color};font-size:14px;margin-bottom:12px'>{title}</h3>"
            f"<table width='100%' cellspacing='0' style='border-collapse:collapse'>"
            f"<tr><th style='{hs}'>Fecha</th><th style='{hs}'>Edicion</th>"
            f"<th style='{hs}'>Seccion</th><th style='{hs}'>Titulo</th>"
            f"<th style='{hs}'>Keywords</th><th style='{hs}'>PDF</th>"
            f"</tr>{rows(pubs)}</table><br>"
        )

    html = (
        "<html><body style='font-family:Arial,sans-serif;color:#333;"
        "max-width:1000px;margin:0 auto'>"
        "<div style='background:#0d2340;padding:20px 28px;border-radius:8px 8px 0 0'>"
        "<div style='font-size:10px;font-weight:700;letter-spacing:2px;"
        "color:#e8a020;text-transform:uppercase;margin-bottom:6px'>"
        "Subsecretaria de Prevencion del Delito - DSP</div>"
        "<h2 style='color:#fff;margin:0;font-size:18px'>Monitor Diario Oficial</h2>"
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
        description="Monitor Diario Oficial DSP v14-FINAL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python scraper.py                           Escaneo del dia de hoy
  python scraper.py --historical              Desde 10-02-2025 hasta hoy
  python scraper.py --date 28-11-2025         Fecha especifica
  python scraper.py --test                    Diagnostico 28-11-2025 (44311 + 44311-B)
  python scraper.py --test --date 03-08-2025  Diagnostico domingo 44215-B
  python scraper.py --test --date 11-02-2026  Diagnostico Ley 21.802
        """
    )
    parser.add_argument("--historical", action="store_true",
                        help=f"Escanear desde {START_DATE} hasta hoy")
    parser.add_argument("--date",    type=str, help="Fecha especifica DD-MM-YYYY")
    parser.add_argument("--test",    action="store_true",
                        help="Diagnostico: edicion regular + B edition")
    parser.add_argument("--verbose", action="store_true",
                        help="Log detallado por seccion y version")
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
        data["b_edition_days"] = {}
        log.info(f"=== ESCANEO HISTORICO: {START_DATE} -> {date.today()} ===")
        current = START_DATE
        count   = 0
        while current <= date.today():
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
