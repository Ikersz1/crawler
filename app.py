import os, re, fnmatch, time
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, urlunparse

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

from playwright.sync_api import sync_playwright

# =========================
#   MODELOS DE ENTRADA
# =========================
class CrawlConfig(BaseModel):
    name: str = Field(default="job")
    startUrls: List[str] = Field(default_factory=list)
    maxPages: int = 100
    maxDepth: int = 3
    sameDomainOnly: bool = True
    includeSubdomains: bool = False
    includePatterns: List[str] = Field(default_factory=lambda: ["**/*"])
    excludePatterns: List[str] = Field(default_factory=lambda: ["**/*?*utm_*", "**/calendar/**", "**/wp-admin/**"])
    useSitemap: bool = False
    prioritizeSitemap: bool = False
    renderJs: bool = True
    timeout: int = 10000
    retries: int = 2
    concurrency: int = 2
    requestsPerMinute: int = 60
    userAgent: str = "WebCrawler/1.0 (+https://example.com/bot)"
    headers: Dict[str, str] = Field(default_factory=dict)
    stripQueryParams: bool = True
    canonicalOnly: bool = False
    extractTitle: bool = True
    extractMetaDescription: bool = True
    extractHeadings: bool = True
    extractMainContent: bool = True
    extractLinks: bool = True
    extractImages: bool = True
    saveRawHtml: bool = False

# =========================
#   UTILS / NORMALIZACIÓN
# =========================
def path_matches_any(path: str, patterns: List[str]) -> bool:
    for patt in patterns:
        if fnmatch.fnmatch(path, patt):
            return True
    return False

def same_or_subdomain(host: str, base_host: str, include_subdomains: bool) -> bool:
    if host == base_host:
        return True
    if include_subdomains and host.endswith("." + base_host):
        return True
    return False

def normalize_url(base_url: str, link: str, cfg: CrawlConfig) -> Optional[str]:
    abs_url = urljoin(base_url, link)
    parsed_base = urlparse(base_url)
    parsed = urlparse(abs_url)

    if cfg.sameDomainOnly:
        if not same_or_subdomain(parsed.netloc.lower(), parsed_base.netloc.lower(), cfg.includeSubdomains):
            return None

    scheme = "https" if parsed.scheme in ("http", "https") else parsed.scheme
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    query_dict = parse_qs(parsed.query)

    query_dict = {k: v for k, v in query_dict.items()
                  if not k.lower().startswith("utm") and k not in ["fbclid", "gclid"]}

    if cfg.stripQueryParams:
        query_dict = {}

    clean_query = "&".join(f"{k}={v[0]}" for k, v in query_dict.items())
    normalized = urlunparse((scheme, netloc, path, "", clean_query, ""))

    if any(normalized.lower().endswith(ext) for ext in
           [".jpg",".jpeg",".png",".gif",".svg",".webp",".css",".js",".pdf",".zip",
            ".mp4",".mp3",".mov",".avi",".woff",".woff2",".ttf",".ico",".rss",".xml"]):
        return None

    if cfg.excludePatterns and path_matches_any(normalized, cfg.excludePatterns):
        return None
    if cfg.includePatterns and not path_matches_any(normalized, cfg.includePatterns):
        return None

    return normalized

def extract_links(page, base_url: str, cfg: CrawlConfig) -> List[str]:
    if not cfg.extractLinks:
        return []
    out, seen = [], set()
    for a in page.query_selector_all("a[href]"):
        href = a.get_attribute("href")
        if not href:
            continue
        link = normalize_url(base_url, href, cfg)
        if link and link not in seen:
            seen.add(link)
            out.append(link)
    return out

def find_tab_elements(page):
    selectors = [
        "[role='tab']",
        ".nav-tabs a, ul.nav-tabs a",
        ".tabs a, ul.tabs a",
        "[data-toggle='tab']",
        "a[href^='#']"
    ]
    tabs, seen_keys = [], set()
    for sel in selectors:
        try:
            handles = page.query_selector_all(sel)
        except:
            handles = []
        for h in handles:
            try:
                text = (h.inner_text() or "").strip()
                href = h.get_attribute("href") or ""
                key = (text, href)
                if not text and not href:
                    continue
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                if href in ("#", "#top", "#!"):
                    if not (h.get_attribute("aria-controls") or h.get_attribute("data-target")):
                        continue
                tabs.append(h)
            except:
                continue
    return tabs

def get_main_content(page, url: str, is_home: bool, cfg: CrawlConfig) -> str:
    if not cfg.extractMainContent:
        return page.inner_text("body")
    if is_home:
        return page.inner_text("body")

    for sel in ["main", "article", "section", "#content", ".content", ".main"]:
        try:
            if page.query_selector(sel):
                return page.inner_text(sel)
        except:
            continue

    return page.evaluate("""
        () => {
            let clone = document.body.cloneNode(true);
            let junk = clone.querySelectorAll("header, footer, nav, aside");
            junk.forEach(el => el.remove());
            return clone.innerText;
        }
    """)

def click_tabs_and_collect(page, url: str, cfg: CrawlConfig) -> Tuple[Dict[str, str], List[str]]:
    sections = {}
    all_links: List[str] = []
    parsed = urlparse(url)
    is_home = (url.rstrip("/") == f"{parsed.scheme}://{parsed.netloc}")

    if not cfg.renderJs:
        sections["Main"] = get_main_content(page, url, is_home, cfg)
        all_links = extract_links(page, url, cfg)
        return sections, all_links

    tabs = find_tab_elements(page)
    if not tabs:
        sections["Main"] = get_main_content(page, url, is_home, cfg)
        all_links = extract_links(page, url, cfg)
        return sections, all_links

    sections["Default"] = get_main_content(page, url, is_home, cfg)
    all_links.extend(extract_links(page, url, cfg))

    for tab in tabs:
        try:
            name = (tab.inner_text() or "").strip() or (tab.get_attribute("aria-controls") or "Tab")
            page.evaluate("(el) => el.click()", tab)
            page.wait_for_timeout(700)

            panel_id = (tab.get_attribute("aria-controls") or "").strip()
            content = None
            if panel_id:
                content = page.evaluate("""
                    (pid) => {
                        try {
                            let sel = "#" + CSS.escape(pid);
                            let el = document.querySelector(sel);
                            return el ? el.innerText : null;
                        } catch {
                            return null;
                        }
                    }
                """, panel_id)

            if not content:
                content = get_main_content(page, url, is_home, cfg)

            if content and content.strip() and content not in sections.values():
                sections[name] = content

            for lk in extract_links(page, url, cfg):
                if lk not in all_links:
                    all_links.append(lk)

        except Exception as e:
            print(f"[Tab Error] {url}: {e}")
            continue

    return sections, all_links

def save_page_markdown(job_dir: str,
                       url: str,
                       title: str,
                       sections: Dict[str, str],
                       html_snippet: str,
                       links: List[str],
                       save_raw_html: bool):
    os.makedirs(job_dir, exist_ok=True)
    safe_name = url.replace("https://", "").replace("http://", "").replace("/", "_")
    md_file = os.path.join(job_dir, f"{safe_name}.md")
    with open(md_file, "w", encoding="utf-8") as f:
        f.write(f"# {title}\n\n")
        f.write(f"**URL:** {url}\n\n")
        for sec_name, sec_text in sections.items():
            f.write(f"## Sección: {sec_name}\n\n")
            f.write((sec_text or "").strip() + "\n\n")
        f.write("## HTML (snippet)\n\n")
        f.write("```\n" + html_snippet[:2000] + "\n... (truncado)\n```\n\n")
        f.write("## Enlaces encontrados (únicos)\n")
        for link in links:
            f.write(f"- {link}\n")

    if save_raw_html:
        html_file = os.path.join(job_dir, f"{safe_name}.html")
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html_snippet)

def crawl_single(start_url: str, cfg: CrawlConfig, output_root: str):
    visited = set()
    to_visit: List[Tuple[str, int]] = [(start_url, 0)]
    enqueued = set([start_url])

    stamp = time.strftime("%Y%m%d-%H%M%S")
    job_dir = os.path.join(output_root, f"{cfg.name}-{stamp}")
    os.makedirs(job_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(user_agent=cfg.userAgent, extra_http_headers=cfg.headers or {})
        page = context.new_page()

        while to_visit and len(visited) < cfg.maxPages:
            url, depth = to_visit.pop(0)
            if url in visited or depth > cfg.maxDepth:
                continue

            print(f"[Crawling] {url}")
            ok, err = False, ""
            for _ in range(max(cfg.retries, 1)):
                try:
                    page.goto(url, timeout=cfg.timeout)
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(300)
                    ok = True
                    break
                except Exception as e:
                    err = str(e)
            if not ok:
                print(f"[Error] {url}: {err}")
                continue

            title = page.title() if cfg.extractTitle else url
            html = page.content()

            sections, links_all_states = click_tabs_and_collect(page, url, cfg)

            # construir la lista SOLO de "siguientes" para este documento
            next_links: List[str] = []
            for href in links_all_states:
                link = normalize_url(url, href, cfg)
                if not link:
                    continue
                if link == url or link in visited or link in enqueued:
                    continue
                to_visit.append((link, depth + 1))
                enqueued.add(link)
                next_links.append(link)

            save_page_markdown(job_dir, url, title, sections, html, next_links, cfg.saveRawHtml)
            visited.add(url)

        browser.close()

    return {"jobDir": job_dir, "pages": len(visited)}

# =========================
#    FASTAPI APP
# =========================
app = FastAPI(title="Crawler API")

# --- CORS configurable por ENV ---
DEFAULT_ORIGINS = [
    "http://localhost:5173", "http://127.0.0.1:5173",
    "http://localhost:3000", "http://127.0.0.1:3000"
]
extra = os.environ.get("CORS_EXTRA_ORIGINS", "")
EXTRA_ORIGINS = [o.strip() for o in extra.split(",") if o.strip()]
ALLOWED = DEFAULT_ORIGINS + EXTRA_ORIGINS

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", "output_crawler")

@app.post("/crawl")
@app.post("/crawl/")
def start_crawl(cfg: CrawlConfig):
    if not cfg.startUrls:
        return {"status": "error", "message": "startUrls vacío"}
    results = []
    for url in cfg.startUrls:
        res = crawl_single(url, cfg, OUTPUT_ROOT)
        results.append({"startUrl": url, **res})
    return {"status": "ok", "results": results}

@app.get("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "7860"))   # <- importante para HF Spaces
    uvicorn.run(app, host="0.0.0.0", port=port)  # <- escucha público
