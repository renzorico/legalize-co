#!/usr/bin/env python3
"""Scrape Colombian laws from SUIN-Juriscol into Legalize Markdown files."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import html
import json
import logging
import random
import re
import sys
import threading
import time
import unicodedata
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag, XMLParsedAsHTMLWarning
from markdownify import markdownify as html_to_markdown


BASE_URL = "https://www.suin-juriscol.gov.co/"
DETAIL_PATH = "viewDocument.asp"
SEARCH_ENDPOINT = urljoin(BASE_URL, "CiclopeWs/Ciclope.svc/Find")
OUTPUT_DIR = Path("laws/co")
USER_AGENT = (
    "legalize-co/0.1 "
    "(https://github.com/legalize-dev/legalize; legalize@legalize.dev)"
)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

SEARCH_TYPES = (
    "Leyes",
    "Decretos",
    "Resolucion",
    "Circular",
    "Acto",
    "Constitucion",
    "DirectivasP",
    "Acuerdo",
    "Instruccion",
)

RANGO_MAP = {
    "ACTO": "acto_legislativo",
    "ACTO LEGISLATIVO": "acto_legislativo",
    "CIRCULAR": "circular",
    "CONSTITUCION": "constitucion",
    "CONSTITUCIÓN": "constitucion",
    "DECRETO": "decreto",
    "DIRECTIVA": "directiva",
    "DIRECTIVA PRESIDENCIAL": "directiva_presidencial",
    "LEY": "ley",
    "RESOLUCION": "resolucion",
    "RESOLUCIÓN": "resolucion",
}

SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

@dataclasses.dataclass(frozen=True)
class Law:
    titulo: str
    identificador: str
    rango: str
    fecha_publicacion: str
    ultima_actualizacion: str
    estado: str
    fuente: str
    body_markdown: str


@dataclasses.dataclass(frozen=True)
class ScrapeResult:
    status: str
    id_: int | None = None
    identifier: str | None = None
    error: str | None = None


@dataclasses.dataclass
class EnumerationStats:
    enumerated: int = 0
    found: int = 0
    skipped: int = 0
    errors: int = 0


class FetchError(RuntimeError):
    """Raised when a SUIN document cannot be fetched or parsed."""


THREAD_LOCAL = threading.local()
PRINT_LOCK = threading.Lock()


def build_session(timeout: float, verify_tls: bool) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-CO,es;q=0.9,en;q=0.7",
            "User-Agent": USER_AGENT,
        }
    )
    session.request_timeout = timeout  # type: ignore[attr-defined]
    session.verify = verify_tls
    return session


def polite_sleep(delay: float, jitter: float = 0.35) -> None:
    if delay <= 0:
        return
    time.sleep(delay + random.uniform(0, jitter))


def fetch_url(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=session.request_timeout)  # type: ignore[attr-defined]
    response.raise_for_status()
    return decode_response(response)


def decode_response(response: requests.Response) -> str:
    """Decode SUIN pages that inconsistently advertise UTF-16, UTF-8, or ISO-8859-1."""
    encodings = [
        response.encoding,
        response.apparent_encoding,
        "utf-8",
        "iso-8859-1",
        "windows-1252",
    ]
    for encoding in dict.fromkeys(filter(None, encodings)):
        try:
            text = response.content.decode(str(encoding), errors="replace")
        except LookupError:
            continue
        if "\ufffd" not in text[:5000]:
            return text
    return response.text


def normalize_space(value: str) -> str:
    value = html.unescape(value or "")
    value = value.replace("\xa0", " ")
    value = re.sub(r"[ \t\r\f\v]+", " ", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def parse_date(value: str | None) -> str | None:
    if not value:
        return None
    value = normalize_space(strip_accents(value)).lower()

    numeric = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", value)
    if numeric:
        day, month, year = map(int, numeric.groups())
        return dt.date(year, month, day).isoformat()

    spanish = re.search(
        r"\b(\d{1,2})\s+de\s+([a-z]+)\s+de\s+(\d{4})\b",
        value,
        flags=re.IGNORECASE,
    )
    if spanish:
        day_text, month_text, year_text = spanish.groups()
        month = SPANISH_MONTHS.get(month_text)
        if month:
            return dt.date(int(year_text), month, int(day_text)).isoformat()

    compact = re.search(r"\b(\d{4})(\d{2})(\d{2})\b", value)
    if compact:
        year, month, day = map(int, compact.groups())
        return dt.date(year, month, day).isoformat()

    return None


def yaml_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def detail_url_from_path(path: str) -> str:
    if path.startswith("http"):
        return path
    if path.startswith("/"):
        return urljoin(BASE_URL, path)
    if path.lower().startswith(("leyes/", "decretos/", "resolucion/", "circular/")):
        return urljoin(BASE_URL, f"{DETAIL_PATH}?ruta={path}")
    return urljoin(BASE_URL, path)


def discover_from_search_api(
    session: requests.Session,
    law_types: Iterable[str],
    max_pages: int | None,
    delay: float,
) -> Iterable[str]:
    """Yield detail URLs from SUIN's internal search endpoint.

    The public form posts this JSON shape to /CiclopeWs/Ciclope.svc/Find.
    The endpoint is fragile, so failures are logged and callers can still use
    --seed-url or --seed-file for detail-page scraping.
    """
    for law_type in law_types:
        query: dict[str, object] = {
            "form": "normatividad",
            "tipo": law_type,
            "fields": "tipo|numero|anio|sector|entidad_emisora|estado_documento|epigrafe",
            "hitlist": "legis",
            "coleccion": "legis",
            "pageSize": 20,
            "usuario": "web",
            "passwd": "dA4qd1uUGLLtM6IK+1xiVQ==",
        }
        page_count = 0

        while True:
            page_count += 1
            if max_pages is not None and page_count > max_pages:
                break

            try:
                response = session.post(
                    SEARCH_ENDPOINT,
                    data=json.dumps(query, ensure_ascii=False).encode("utf-8"),
                    headers={
                        "Content-Type": "json",
                        "Referer": urljoin(BASE_URL, "legislacion/normatividad.html"),
                    },
                    timeout=session.request_timeout,  # type: ignore[attr-defined]
                )
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:
                logging.warning("search failed for %s page %s: %s", law_type, page_count, exc)
                break

            error = payload.get("error")
            if error:
                logging.warning("search failed for %s page %s: %s", law_type, page_count, error)
                break

            docs = payload.get("docs") or []
            for item in docs:
                path = item.get("path")
                if path:
                    yield detail_url_from_path(str(path))

            if payload.get("viewIsEnd") != "no" or not docs:
                break

            query["cookies"] = payload.get("cookies")
            query["pagina"] = "next"
            polite_sleep(delay)


def discover_from_listing_pages(
    session: requests.Session,
    listing_urls: Iterable[str],
    max_pages: int | None,
    delay: float,
) -> Iterable[str]:
    """Extract detail links from HTML listing pages and follow simple next links."""
    seen_pages: set[str] = set()
    queue = list(listing_urls)
    page_count = 0

    while queue:
        url = queue.pop(0)
        if url in seen_pages:
            continue
        seen_pages.add(url)
        page_count += 1
        if max_pages is not None and page_count > max_pages:
            break

        try:
            html_text = fetch_url(session, url)
        except requests.RequestException as exc:
            logging.warning("listing fetch failed %s: %s", url, exc)
            continue

        soup = BeautifulSoup(html_text, "lxml")
        for anchor in soup.select("a[href]"):
            href = anchor.get("href", "")
            if "viewDocument.asp" in href:
                yield urljoin(BASE_URL, href)
            elif href.lower().startswith(("leyes/", "decretos/")):
                yield detail_url_from_path(href)

        next_link = find_next_link(soup, url)
        if next_link:
            queue.append(next_link)
        polite_sleep(delay)


def find_next_link(soup: BeautifulSoup, current_url: str) -> str | None:
    for anchor in soup.select("a[href]"):
        text = normalize_space(anchor.get_text(" "))
        href = anchor.get("href", "")
        if text.lower() in {"siguiente", "next", ">"} or "pagina=next" in href.lower():
            return urljoin(current_url, href)
    return None


def canonicalize_detail_url(url: str) -> str:
    parsed = urlparse(urljoin(BASE_URL, url))
    if parsed.path.lower().endswith("/viewdocument.asp"):
        query = parse_qs(parsed.query)
        if "id" in query:
            return urljoin(BASE_URL, f"{DETAIL_PATH}?id={query['id'][0]}")
        if "ruta" in query:
            return urljoin(BASE_URL, f"{DETAIL_PATH}?{urlencode({'ruta': query['ruta'][0]})}")
    return parsed.geturl()


def parse_law_page(html_text: str, source_url: str) -> Law:
    soup = BeautifulSoup(html_text, "lxml")
    remove_noise(soup)

    full_text = normalize_space(soup.get_text("\n"))
    title = extract_title(soup, full_text)
    rango, number, year = extract_identifier_parts(title, full_text)
    identifier = f"{rango.upper()}-{number}-{year}"
    rango_value = RANGO_MAP.get(rango.upper(), strip_accents(rango).lower().replace(" ", "_"))

    publication_date = extract_labeled_date(full_text, "Fecha de publicación de la norma")
    if not publication_date:
        publication_date = extract_first_date_near_title(full_text) or f"{year}-01-01"

    updated_date = (
        extract_labeled_date(full_text, "Última actualización")
        or extract_labeled_date(full_text, "Ultima actualizacion")
        or publication_date
    )

    estado = extract_status(full_text)
    body_markdown = extract_body_markdown(soup)
    if not body_markdown or len(body_markdown) < 120:
        raise FetchError(f"body unavailable for {source_url}")

    return Law(
        titulo=title,
        identificador=identifier,
        rango=rango_value,
        fecha_publicacion=publication_date,
        ultima_actualizacion=updated_date,
        estado=estado,
        fuente=canonicalize_detail_url(source_url),
        body_markdown=body_markdown,
    )


def remove_noise(soup: BeautifulSoup) -> None:
    for selector in (
        "script",
        "style",
        "header",
        "nav",
        "button",
        ".panel",
        "#toolPanel",
        "#toolFab",
        ".fab",
    ):
        for element in soup.select(selector):
            element.decompose()


def extract_title(soup: BeautifulSoup, full_text: str) -> str:
    meta_description = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    if isinstance(meta_description, Tag):
        content = meta_description.get("content", "")
        match = re.match(r"\s*([^,|]+?)\s*-\s*Colombia", content)
        if match:
            heading = normalize_space(match.group(1))
            epigraph = content.split(",", 2)[-1] if "," in content else ""
            if epigraph and "SUIN" not in epigraph:
                return normalize_space(f"{heading} - {epigraph}")

    title_tag = soup.find("title")
    if title_tag:
        title = normalize_space(title_tag.get_text(" "))
        title = re.sub(r"\s*-\s*Colombia\s*\|\s*SUIN Juriscol\s*$", "", title)
        if title:
            epigraph = extract_epigraph(full_text)
            return normalize_space(f"{title} - {epigraph}") if epigraph else title

    match = re.search(
        r"\b((?:LEY|DECRETO|RESOLUCI[ÓO]N|CIRCULAR|ACTO LEGISLATIVO)\s+\d+\s+DE\s+\d{4})\b",
        full_text,
        flags=re.IGNORECASE,
    )
    if match:
        epigraph = extract_epigraph(full_text)
        return normalize_space(f"{match.group(1).upper()} - {epigraph}") if epigraph else match.group(1).upper()

    raise FetchError("could not extract title")


def extract_epigraph(full_text: str) -> str | None:
    lines = [normalize_space(line) for line in full_text.splitlines()]
    for index, line in enumerate(lines):
        if re.match(r"^\(?[a-záéíóú]+\s+\d{1,2}\)?$", line.lower()):
            for candidate in lines[index + 1 : index + 5]:
                if candidate and not candidate.lower().startswith("estado de vigencia"):
                    return candidate
    return None


def extract_identifier_parts(title: str, full_text: str) -> tuple[str, str, str]:
    pattern = (
        r"\b(LEY|DECRETO|RESOLUCI[ÓO]N|CIRCULAR|ACTO(?:\s+LEGISLATIVO)?|CONSTITUCI[ÓO]N)"
        r"\s+(\d+)\s+DE\s+(\d{4})\b"
    )
    for text in (title, full_text):
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            rango, number, year = match.groups()
            rango = strip_accents(rango).upper()
            if rango == "ACTO":
                rango = "ACTO LEGISLATIVO"
            return rango, number.lstrip("0") or "0", year
    raise FetchError("could not extract identifier")


def extract_labeled_date(full_text: str, label: str) -> str | None:
    normalized = strip_accents(full_text)
    normalized_label = strip_accents(label)
    pattern = rf"{re.escape(normalized_label)}\s*[:\n ]+\s*([0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}})"
    match = re.search(pattern, normalized, flags=re.IGNORECASE)
    return parse_date(match.group(1)) if match else None


def extract_first_date_near_title(full_text: str) -> str | None:
    lines = [normalize_space(line) for line in full_text.splitlines() if normalize_space(line)]
    for line in lines[:80]:
        parsed = parse_date(line)
        if parsed:
            return parsed
    return None


def extract_status(full_text: str) -> str:
    match = re.search(r"ESTADO DE VIGENCIA\s*:\s*([^\n\[]+)", full_text, flags=re.IGNORECASE)
    status_text = strip_accents(match.group(1)).lower() if match else strip_accents(full_text).lower()
    if "parcial" in status_text and "derog" in status_text:
        return "parcialmente_derogada"
    if "derog" in status_text and "vigente" not in status_text:
        return "derogada"
    return "vigente"


def extract_body_markdown(soup: BeautifulSoup) -> str:
    body = soup.body or soup
    body_copy = BeautifulSoup(str(body), "lxml")
    remove_noise(body_copy)

    for selector in (".toc", ".toctitle", "ul.lista-toc", "ul.resumenvigencias"):
        for element in body_copy.select(selector):
            element.decompose()

    html_text = str(body_copy.body or body_copy)

    markdown = html_to_markdown(
        html_text,
        heading_style="ATX",
        bullets="-",
        strip=("img", "script", "style"),
    )
    markdown = clean_markdown(markdown)

    start = re.search(
        r"(?im)^#{0,6}\s*(LEY|DECRETO|RESOLUCI[ÓO]N|CIRCULAR|ACTO LEGISLATIVO)"
        r"\s+\d+\s+DE\s+\d{4}",
        markdown,
    )
    if start:
        markdown = markdown[start.start() :]

    return markdown.strip()


def clean_markdown(markdown: str) -> str:
    markdown = markdown.replace("\xa0", " ")
    markdown = re.sub(r"\[Mostrar\]\([^)]*\)", "", markdown)
    markdown = re.sub(r"\[\s*Mostrar\s*\]", "", markdown, flags=re.IGNORECASE)
    markdown = re.sub(r"\n[ \t]+", "\n", markdown)
    markdown = re.sub(r"[ \t]{2,}", " ", markdown)
    markdown = re.sub(r"\n{3,}", "\n\n", markdown)
    return normalize_space(markdown)


def render_law(law: Law) -> str:
    frontmatter = {
        "titulo": law.titulo,
        "identificador": law.identificador,
        "pais": "co",
        "rango": law.rango,
        "fecha_publicacion": law.fecha_publicacion,
        "ultima_actualizacion": law.ultima_actualizacion,
        "estado": law.estado,
        "fuente": law.fuente,
    }
    lines = ["---"]
    for key, value in frontmatter.items():
        lines.append(f"{key}: {yaml_quote(value)}")
    lines.extend(["---", "", law.body_markdown, ""])
    return "\n".join(lines)


def write_law(law: Law, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{law.identificador}.md"
    path.write_text(render_law(law), encoding="utf-8")
    return path


def scrape_detail_page(url: str, session: requests.Session, output_dir: Path) -> Law:
    page = fetch_url(session, url)
    law = parse_law_page(page, url)
    write_law(law, output_dir)
    return law


def load_checkpoint(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logging.warning("checkpoint is invalid, starting from --id-start: %s", exc)
        return None
    last_id = payload.get("last_id")
    if isinstance(last_id, int) or str(last_id).isdigit():
        return int(last_id) + 1
    return None


def save_checkpoint(path: Path, id_: int) -> None:
    path.write_text(json.dumps({"last_id": id_}, ensure_ascii=False) + "\n", encoding="utf-8")


def source_ids_already_written(output_dir: Path) -> set[int]:
    ids: set[int] = set()
    if not output_dir.exists():
        return ids
    source_re = re.compile(r"fuente:\s*[\"']?[^\"'\n]*viewDocument\.asp\?id=(\d+)", re.IGNORECASE)
    for path in output_dir.glob("*.md"):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        match = source_re.search(text)
        if match:
            ids.add(int(match.group(1)))
    return ids


def get_thread_session(args: argparse.Namespace) -> requests.Session:
    session = getattr(THREAD_LOCAL, "session", None)
    if session is None:
        session = build_session(args.timeout, verify_tls=not args.no_verify_tls)
        THREAD_LOCAL.session = session
    return session


def is_law_detail_page(html_text: str) -> bool:
    soup = BeautifulSoup(html_text, "lxml")
    if soup.find(id="tituloDoc"):
        return True
    title = soup.find("title")
    title_text = normalize_space(title.get_text(" ")) if title else ""
    return bool(
        re.search(
            r"\b(LEY|DECRETO|RESOLUCI[ÓO]N|CIRCULAR|ACTO LEGISLATIVO)\s+\d+\s+DE\s+\d{4}\b",
            title_text,
            flags=re.IGNORECASE,
        )
    )


def process_id(id_: int, args: argparse.Namespace, existing_ids: set[int]) -> ScrapeResult:
    if id_ in existing_ids:
        with PRINT_LOCK:
            print(f"✗ id={id_} (already exists)")
        return ScrapeResult(status="skipped", id_=id_)

    url = f"{BASE_URL}{DETAIL_PATH}?id={id_}"
    session = get_thread_session(args)
    try:
        head = session.head(
            url,
            allow_redirects=True,
            timeout=session.request_timeout,  # type: ignore[attr-defined]
        )
        if head.status_code != 200:
            with PRINT_LOCK:
                print(f"✗ id={id_} (no law)")
            return ScrapeResult(status="skipped", id_=id_)

        page = fetch_url(session, url)
        if not is_law_detail_page(page):
            with PRINT_LOCK:
                print(f"✗ id={id_} (no law)")
            return ScrapeResult(status="skipped", id_=id_)

        law = parse_law_page(page, url)
        write_law(law, args.output_dir)
    except Exception as exc:
        with PRINT_LOCK:
            print(f"✗ id={id_} (error: {exc})")
        return ScrapeResult(status="error", id_=id_, error=str(exc))

    with PRINT_LOCK:
        print(f"✓ id={id_} → {law.identificador}")
    return ScrapeResult(status="found", id_=id_, identifier=law.identificador)


def enumerate_ids(args: argparse.Namespace) -> EnumerationStats:
    checkpoint = Path(args.checkpoint)
    checkpoint_start = load_checkpoint(checkpoint)
    start = max(args.id_start, checkpoint_start) if checkpoint_start else args.id_start
    existing_ids = source_ids_already_written(args.output_dir)
    stats = EnumerationStats()
    futures = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for id_ in range(start, args.id_end + 1):
            stats.enumerated += 1
            futures.append(pool.submit(process_id, id_, args, existing_ids))
            save_checkpoint(checkpoint, id_)
            time.sleep(args.delay / args.workers)

        for future in as_completed(futures):
            result = future.result()
            if result.status == "found":
                stats.found += 1
            elif result.status == "skipped":
                stats.skipped += 1
            else:
                stats.errors += 1

    print(
        f"Enumerated {stats.enumerated} ids: "
        f"{stats.found} found, {stats.skipped} skipped, {stats.errors} errors"
    )
    return stats


def iter_seed_urls(args: argparse.Namespace) -> Iterable[str]:
    for url in args.seed_url:
        yield url
    for seed_file in args.seed_file:
        with Path(seed_file).open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line and not line.startswith("#"):
                    yield line


def run(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )
    session = build_session(args.timeout, verify_tls=not args.no_verify_tls)
    if args.no_verify_tls:
        requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]
        logging.warning("TLS verification is disabled for this run")

    if args.enumerate:
        enumerate_ids(args)
        return 0

    urls: list[str] = []
    urls.extend(canonicalize_detail_url(url) for url in iter_seed_urls(args))

    if args.listing_url:
        urls.extend(
            canonicalize_detail_url(url)
            for url in discover_from_listing_pages(
                session=session,
                listing_urls=args.listing_url,
                max_pages=args.max_pages,
                delay=args.delay,
            )
        )

    if args.discover:
        urls.extend(
            canonicalize_detail_url(url)
            for url in discover_from_search_api(
                session=session,
                law_types=args.law_type,
                max_pages=args.max_pages,
                delay=args.delay,
            )
        )

    unique_urls = list(dict.fromkeys(urls))
    if args.limit:
        unique_urls = unique_urls[: args.limit]

    if not unique_urls:
        logging.error("no detail URLs found; pass --discover, --listing-url, --seed-url, or --seed-file")
        return 2

    written = 0
    skipped = 0
    for index, url in enumerate(unique_urls, start=1):
        logging.info("fetching %s/%s %s", index, len(unique_urls), url)
        try:
            law = scrape_detail_page(url, session, args.output_dir)
        except Exception as exc:
            skipped += 1
            logging.warning("skipped %s: %s", url, exc)
        else:
            written += 1
            logging.info("wrote %s/%s.md", args.output_dir, law.identificador)
        polite_sleep(args.delay)

    logging.info("done: wrote=%s skipped=%s", written, skipped)
    return 0 if written else 1


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--discover", action="store_true", help="discover documents through SUIN search")
    parser.add_argument("--enumerate", action="store_true", help="enumerate SUIN detail IDs")
    parser.add_argument("--id-start", type=int, default=1700000)
    parser.add_argument("--id-end", type=int, default=1900000)
    parser.add_argument("--checkpoint", type=Path, default=Path("checkpoint.json"))
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--law-type", action="append", choices=SEARCH_TYPES, default=[])
    parser.add_argument("--listing-url", action="append", default=[], help="HTML listing page to crawl")
    parser.add_argument("--seed-url", action="append", default=[], help="direct SUIN detail URL")
    parser.add_argument("--seed-file", action="append", default=[], help="file containing one detail URL per line")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--limit", type=int, help="maximum number of detail URLs to process")
    parser.add_argument("--max-pages", type=int, help="maximum listing/search pages to discover")
    parser.add_argument("--delay", type=float, default=1.0, help="polite delay between requests")
    parser.add_argument("--timeout", type=float, default=45.0)
    parser.add_argument("--no-verify-tls", action="store_true", help="disable TLS verification for SUIN")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    if args.id_end < args.id_start:
        parser.error("--id-end must be greater than or equal to --id-start")
    if not args.law_type:
        args.law_type = list(SEARCH_TYPES)
    return args


if __name__ == "__main__":
    raise SystemExit(run(parse_args(sys.argv[1:])))
