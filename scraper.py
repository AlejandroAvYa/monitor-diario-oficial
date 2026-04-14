"""
scraper.py — Monitor Diario Oficial Chile
División de Seguridad Privada (DSP)

Uso:
  python scraper.py               → Escanea solo el día de hoy
  python scraper.py --historical  → Escanea desde 15-02-2025 hasta hoy
  python scraper.py --date 13-04-2026  → Escanea una fecha específica
"""

import json
import os
import re
import sys
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

BASE_DIR       = Path(__file__).parent
DATA_FILE      = BASE_DIR / "data" / "publications.json"
KEYWORDS_FILE  = BASE_DIR / "keywords.json"

BASE_URL       = "https://www.diariooficial.interior.gob.cl/edicionelectronica"
START_DATE     = date(2025, 2, 15)
ANCHOR_DATE    = date(2026, 4, 13)
ANCHOR_EDITION = 44423
REQUEST_DELAY  = 2.0   # segundos entre requests

SECTIONS = {
    "Normas Generales":         "index.php",
    "Normas Particulares":      "normas_particulares.php",
    "Publicaciones Judiciales": "publicaciones_judiciales.php",
    "Avisos Destacados":        "avisos_destacados.php",
}

MONTHS_ES = {
    "enero":"01","febrero":"02","marzo":"03","abril":"04",
    "mayo":"05","junio":"06","julio":"07","agosto":"08",
    "septiembre":"09","octubre":"10","noviembre":"11","diciembre":"12"
}

# Variables de entorno (seteadas como GitHub Secrets)
GMAIL_USER      = os.environ.get("GMAIL_USER", "")
GMAIL_PASSWORD  = os.environ.get("GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL    = os.environ.get("NOTIFY_EMAIL", GMAIL_USER)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# CARGA / GUARDADO DE DATOS
# ──────────────────────────────────────────────────────────────────────────────

def load_data() -> dict:
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_updated": None, "total": 0, "editions_cache": {}, "publications": []}


def save_data(data: dict):
    data["total"] = len(data["publications"])
    data["last_updated"] = date.today().isoformat()
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(f"Datos guardados: {data['total']} publicaciones en total.")


def load_keywords() -> dict:
    with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
        kw = json.load(f)
    return {
        "alta_prioridad": kw.get("alta_prioridad", []),
        "instituciones":  kw.get("instituciones", []),
        "materias":       kw.get("materias", []),
    }


def get_processed_dates(data: dict) -> set:
    return {p["date"] for p in data["publications"]} | \
           set(data.get("skipped_dates", []))


# ──────────────────────────────────────────────────────────────────────────────
# FILTRO DE KEYWORDS
# ──────────────────────────────────────────────────────────────────────────────

def check_keywords(text: str, keywords: dict) -> tuple[list, str]:
    """Retorna (lista de matches, prioridad)."""
    text_lower = text.lower()
    matched = []
    priority = "normal"

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
# EDITION MAPPER
# ──────────────────────────────────────────────────────────────────────────────

def parse_spanish_date(text: str) -> str | None:
    m = re.search(r'(\d+)\s+de\s+(\w+)\s+de\s+(\d{4})', text, re.IGNORECASE)
    if m:
        day, month_es, year = m.groups()
        month = MONTHS_ES.get(month_es.lower())
        if month:
            return f"{int(day):02d}-{month}-{year}"
    return None


def fetch_date_for_edition(session: requests.Session, edition_id: int) -> str | None:
    url = f"{BASE_URL}/index.php?edition={edition_id}"
    try:
        r = session.get(url, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        for li in soup.select("ul li"):
            result = parse_spanish_date(li.get_text(strip=True))
            if result:
                return result
    except Exception as e:
        log.debug(f"Error edición {edition_id}: {e}")
    return None


def find_edition(session: requests.Session, target: date, cache: dict) -> int | None:
    date_str = target.strftime("%d-%m-%Y")

    # 1. Buscar en caché local (guardado en publications.json)
    if date_str in cache:
        return cache[date_str]

    # 2. Estimar desde el punto más cercano conocido en caché
    if cache:
        # Encontrar la fecha conocida más cercana
        def date_from_str(s):
            return date(int(s[6:10]), int(s[3:5]), int(s[:2]))

        nearest_str = min(cache.keys(), key=lambda s: abs((target - date_from_str(s)).days))
        nearest_date = date_from_str(nearest_str)
        nearest_edition = cache[nearest_str]
        delta = (target - nearest_date).days
        estimated = nearest_edition + delta
    else:
        # Usar el ancla hardcodeada
        delta = (target - ANCHOR_DATE).days
        estimated = ANCHOR_EDITION + delta

    # 3. Buscar en ventana ±20 alrededor de la estimación
    for offset in range(0, 21):
        for sign in ([0] if offset == 0 else [1, -1]):
            candidate = estimated + offset * sign
            if candidate < 1:
                continue
            result = fetch_date_for_edition(session, candidate)
            if result == date_str:
                cache[date_str] = candidate
                log.info(f"Edición encontrada: {date_str} → #{candidate}")
                return candidate
            time.sleep(0.3)

    log.warning(f"Sin edición para {date_str} (posible día no hábil)")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# SCRAPER DE SECCIONES
# ──────────────────────────────────────────────────────────────────────────────

def scrape_section(session: requests.Session, url: str) -> list[dict]:
    items = []
    try:
        r = session.get(url, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")
        for row in soup.select("table tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            title = cells[0].get_text(separator=" ", strip=True)
            link  = cells[-1].find("a")
            if not title or not link:
                continue
            href = link.get("href", "")
            link_text = link.get_text(strip=True)
            m = re.search(r'CVE-?(\d+)', link_text + href)
            cve = m.group(1) if m else re.sub(r'[^a-zA-Z0-9]', '', href)[-12:]
            pdf_url = href if href.startswith("http") else \
                f"https://www.diariooficial.interior.gob.cl{href}"
            if title and len(title) > 10:
                items.append({"title": title, "pdf_url": pdf_url, "cve": cve or href})
    except Exception as e:
        log.error(f"Error scrapeando {url}: {e}")
    return items


# ──────────────────────────────────────────────────────────────────────────────
# PROCESAMIENTO DE UN DÍA
# ──────────────────────────────────────────────────────────────────────────────

def process_date(session: requests.Session, target: date,
                 data: dict, keywords: dict) -> list[dict]:
    """Procesa una fecha. Retorna lista de nuevas publicaciones con match."""
    date_str = target.strftime("%d-%m-%Y")
    processed = get_processed_dates(data)

    if date_str in processed:
        log.debug(f"{date_str}: ya procesado, saltando.")
        return []

    edition_id = find_edition(session, target, data.setdefault("editions_cache", {}))
    if not edition_id:
        data.setdefault("skipped_dates", []).append(date_str)
        return []

    log.info(f"Procesando {date_str} (edición #{edition_id})…")
    new_matches = []

    for section_name, php_file in SECTIONS.items():
        url = f"{BASE_URL}/{php_file}?date={date_str}&edition={edition_id}"
        items = scrape_section(session, url)

        for item in items:
            matched_kw, priority = check_keywords(item["title"], keywords)
            if matched_kw:
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
                # Evitar duplicados
                existing_cves = {p["cve"] for p in data["publications"]}
                if item["cve"] not in existing_cves:
                    data["publications"].append(pub)
                    new_matches.append(pub)
                    log.info(f"  ✓ [{priority.upper()}] {section_name}: {item['title'][:80]}")

        time.sleep(REQUEST_DELAY)

    return new_matches


# ──────────────────────────────────────────────────────────────────────────────
# NOTIFICACIONES EMAIL
# ──────────────────────────────────────────────────────────────────────────────

def send_email_alert(new_pubs: list[dict]):
    if not GMAIL_USER or not GMAIL_PASSWORD:
        log.warning("Credenciales de Gmail no configuradas. Saltando notificación.")
        return
    if not new_pubs:
        log.info("Sin publicaciones nuevas para notificar.")
        return

    alta   = [p for p in new_pubs if p["priority"] == "alta"]
    normal = [p for p in new_pubs if p["priority"] == "normal"]

    subject = (
        f"📋 Diario Oficial [{date.today().strftime('%d/%m/%Y')}] — "
        f"{len(alta)} alta prioridad · {len(normal)} normal"
    )

    def table_rows(pubs):
        rows = ""
        for p in pubs:
            kw_str = ", ".join(p["matched_kw"])
            rows += f"""
            <tr>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;font-size:12px;color:#555;white-space:nowrap">{p['date']}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;font-size:11px;color:#888">{p['section']}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;font-size:12.5px">{p['title']}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee;font-size:11px;color:#1a6ab1">{kw_str}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #eee">
                <a href="{p['pdf_url']}" style="color:#c0392b;font-size:12px;font-weight:600;text-decoration:none">PDF →</a>
              </td>
            </tr>"""
        return rows

    header_style = "background:#f7f8fa;color:#555;font-size:11px;font-weight:600;letter-spacing:0.8px;padding:8px 10px;border-bottom:2px solid #ddd;text-align:left;text-transform:uppercase"

    html = f"""
    <html><body style="font-family:Arial,sans-serif;color:#333;max-width:900px;margin:0 auto">
      <div style="background:#0d2340;padding:20px 28px;border-radius:8px 8px 0 0">
        <div style="font-size:10px;font-weight:700;letter-spacing:2px;color:#e8a020;text-transform:uppercase;margin-bottom:6px">
          Subsecretaría de Prevención del Delito · DSP
        </div>
        <h2 style="color:#fff;margin:0;font-size:18px">Monitor Diario Oficial</h2>
        <p style="color:rgba(255,255,255,0.5);margin:4px 0 0;font-size:13px">
          {date.today().strftime('%A %d de %B de %Y')} · {len(new_pubs)} nueva(s) publicación(es) relevante(s)
        </p>
      </div>
      <div style="background:#fff;border:1px solid #e0e4ea;border-top:none;padding:24px;border-radius:0 0 8px 8px">

    {'<h3 style="color:#c0392b;font-size:14px;margin-bottom:12px">⚠️ Alta Prioridad — ' + str(len(alta)) + ' publicación(es)</h3><table width="100%" cellspacing="0" style="border-collapse:collapse"><tr><th style="' + header_style + '">Fecha</th><th style="' + header_style + '">Sección</th><th style="' + header_style + '">Título</th><th style="' + header_style + '">Keywords</th><th style="' + header_style + '">PDF</th></tr>' + table_rows(alta) + '</table><br>' if alta else ""}

    {'<h3 style="color:#1a6ab1;font-size:14px;margin-bottom:12px">📌 Prioridad Normal — ' + str(len(normal)) + ' publicación(es)</h3><table width="100%" cellspacing="0" style="border-collapse:collapse"><tr><th style="' + header_style + '">Fecha</th><th style="' + header_style + '">Sección</th><th style="' + header_style + '">Título</th><th style="' + header_style + '">Keywords</th><th style="' + header_style + '">PDF</th></tr>' + table_rows(normal) + '</table>' if normal else ""}

        <hr style="border:none;border-top:1px solid #eee;margin:24px 0">
        <p style="font-size:11px;color:#aaa;margin:0">
          Generado automáticamente · Monitor Diario Oficial DSP
        </p>
      </div>
    </body></html>
    """

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
    parser = argparse.ArgumentParser(description="Monitor Diario Oficial DSP")
    parser.add_argument("--historical", action="store_true",
                        help=f"Escanear desde {START_DATE} hasta hoy")
    parser.add_argument("--date", type=str,
                        help="Escanear una fecha específica (DD-MM-YYYY)")
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (DSP-Monitor/1.0)"})

    data     = load_data()
    keywords = load_keywords()

    all_new = []

    if args.historical:
        log.info(f"=== ESCANEO HISTÓRICO: {START_DATE} → {date.today()} ===")
        current = START_DATE
        today   = date.today()
        while current <= today:
            if current.weekday() < 5:
                new = process_date(session, current, data, keywords)
                all_new.extend(new)
                # Guardar progreso cada 10 días para no perder datos si hay error
                if (current - START_DATE).days % 10 == 0:
                    save_data(data)
            current += timedelta(days=1)

    elif args.date:
        d = date(int(args.date[6:10]), int(args.date[3:5]), int(args.date[:2]))
        log.info(f"=== ESCANEO FECHA ESPECÍFICA: {args.date} ===")
        all_new = process_date(session, d, data, keywords)

    else:
        today = date.today()
        log.info(f"=== ESCANEO DIARIO: {today} ===")
        if today.weekday() < 5:
            all_new = process_date(session, today, data, keywords)
        else:
            log.info("Hoy es fin de semana, sin edición del DO.")

    save_data(data)

    # Notificar solo si hay publicaciones nuevas
    if all_new:
        send_email_alert(all_new)
        log.info(f"Total nuevas publicaciones con match: {len(all_new)}")
    else:
        log.info("Sin publicaciones nuevas que coincidan con las keywords.")


if __name__ == "__main__":
    main()
