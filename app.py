import os, fnmatch, time, csv, json, re
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, urlunparse

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
import uvicorn

# ---- Playwright ----
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
    renderJs: bool = True                  # si False, evitamos clicks/tabs
    timeout: int = 30000                   # ms para goto
    retries: int = 2
    concurrency: int = 2                   # (no usado en esta versión sync)
    requestsPerMinute: int = 60            # (no usado en esta versión sync)
    userAgent: str = "WebCrawler/1.0 (+https://example.com/bot)"
    headers: Dict[str, str] = Field(default_factory=dict)
    stripQueryParams: bool = True
    canonicalOnly: bool = False            # (placeholder)
    extractTitle: bool = True
    extractMetaDescription: bool = True    # (placeholder)
    extractHeadings: bool = True           # (placeholder)
    extractMainContent: bool = True
    extractLinks: bool = True
    extractImages: bool = True             # (placeholder)
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

    url_for_matching = normalized
    if cfg.excludePatterns and path_matches_any(url_for_matching, cfg.excludePatterns):
        return None

    if cfg.includePatterns and not path_matches_any(url_for_matching, cfg.includePatterns):
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
        if href.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue
        link = normalize_url(base_url, href, cfg)
        if link and link not in seen:
            seen.add(link)
            out.append(link)
    return out


# =========================
#   DETECCIÓN CONTENEDOR + TEXTO VISIBLE
# =========================
def detect_main_container_selector(page) -> str:
    candidates = [
        "main", "[role='main']", "article", "#content", ".content", ".main",
        ".entry-content", ".post-content", ".page-content", ".container", ".site-content"
    ]
    best_sel, best_len = "body", 0
    for sel in candidates:
        try:
            el = page.query_selector(sel)
            if not el:
                continue
            txt = el.inner_text() or ""
            L = len(txt.strip())
            if L > best_len:
                best_len = L
                best_sel = sel
        except:
            continue
    return best_sel

# --- Reemplaza el VISIBLE_TEXT_JS por este (más seguro) ---
VISIBLE_TEXT_JS = """
(sel) => {
  function isVisible(el){
    if(!el) return false;
    const style = getComputedStyle(el);
    if(style.display==='none' || style.visibility==='hidden' || +style.opacity===0) return false;
    const rect = el.getBoundingClientRect();
    if(rect.width<=0 || rect.height<=0) return false;
    return true;
  }
  const root = document.querySelector(sel) || document.body;
  // Tomamos innerText directo del original, pero antes ocultamos cabeceras típicas
  const clone = root.cloneNode(true);
  clone.querySelectorAll('script, style, noscript, header, footer, nav, aside').forEach(n=>n.remove());

  // Si todo el contenedor está invisible, devolvemos cadena vacía
  if(!isVisible(root)) {
    return (root.innerText || '').trim();
  }

  // No hacemos poda por índices (puede desalinear). Usamos innerText del clon.
  const txt = clone.innerText || "";
  const lines = txt.split('\\n').map(s=>s.trim()).filter(Boolean)
    .filter(s=>!(s.startsWith('Home >') || s.toLowerCase().startsWith('share:')));
  return lines.join('\\n').replace(/[ \\t]{2,}/g,' ').trim();
}
"""

def get_main_content(page, url: str, is_home: bool, cfg: CrawlConfig) -> str:
    """
    Intenta texto visible del contenedor principal; si sale corto o vacío,
    cae a inner_text() del mejor candidato; y por último a body.
    """
    if not cfg.extractMainContent:
        return page.inner_text("body")

    # 1) Selector del contenedor más “largo”
    sel = detect_main_container_selector(page)

    # 2) Texto “visible”
    try:
        visible_txt = page.evaluate(VISIBLE_TEXT_JS, sel) or ""
    except:
        visible_txt = ""

    # 3) Si es muy corto/ vacío, probamos inner_text() de candidatos
    if len(visible_txt.strip()) < 40:
        for cand in [sel, "main", "[role='main']", "article", "#content", ".content", ".main", "body"]:
            try:
                if page.query_selector(cand):
                    fallback_txt = page.inner_text(cand) or ""
                    fallback_txt = re.sub(r"[ \\t]{2,}", " ", fallback_txt).strip()
                    if len(fallback_txt) > len(visible_txt):
                        visible_txt = fallback_txt
                        if len(visible_txt) >= 40:
                            break
            except:
                continue

    return visible_txt.strip()



# =========================
#       TABS GENÉRICOS
# =========================
def find_tab_elements(page):
    selectors = [
        "[role='tab']",
        "button[role='tab']",
        ".nav-tabs a, ul.nav-tabs a, .tabs a, ul.tabs a",
        "[data-toggle='tab'], [data-tab], a[href^='#']",
        ".tab, .tab-link, .tabs__nav a, .tabs-nav a, .tab-title a"
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
                role = h.get_attribute("role") or ""
                if href in ("#", "#!", "#top") and not (h.get_attribute("aria-controls") or h.get_attribute("data-target")) and role != "tab":
                    continue
                key = (text, href, role, id(h))
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                tabs.append(h)
            except:
                continue
    return tabs

def normalize_section_name(name: str) -> Optional[str]:
    if not name:
        return None
    name = name.strip()
    if name in {"+", "•", "|"}:
        return None
    if name.lower() == "default":
        return "Main"
    return name

def clean_section_text(text: str) -> str:
    if not text:
        return ""
    lines = []
    for ln in text.splitlines():
        s = ln.strip()
        if not s:
            continue
        if s.startswith("Home >"):
            continue
        if s.lower().startswith("share:"):
            continue
        lines.append(s)
    out = "\n".join(lines)
    out = re.sub(r"[ \t]{2,}", " ", out)
    return out.strip()

def _similar(a: str, b: str) -> float:
    """Similitud simple por superposición de tokens."""
    ta = set(re.findall(r"\w+", a.lower()))
    tb = set(re.findall(r"\w+", b.lower()))
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union



CLEAN_MAIN_WITHOUT_TABS_JS = """
(sel) => {
  function prune(root) {
    root.querySelectorAll('script, style, noscript, header, footer, nav, aside').forEach(n=>n.remove());
    // quita navegación de tabs y paneles típicos
    const tabSelectors = [
      '[role="tablist"]',
      '[role="tabpanel"]',
      '.tab-content',
      '.tab-pane',
      '.tabs',
      '.tabs-nav',
      '.tabs__nav',
      '.tabset',
      '.tabbed-content',
      '.uk-tab',
      '.uk-switcher',
      '.elementor-tabs',
      '.nav-tabs'
    ];
    tabSelectors.forEach(s=>{
      root.querySelectorAll(s).forEach(n=>n.remove());
    });
  }
  const el = document.querySelector(sel) || document.body;
  const clone = el.cloneNode(true);
  prune(clone);
  const txt = (clone.innerText || '').split('\\n').map(s=>s.trim()).filter(Boolean)
               .filter(s=>!(s.startsWith('Home >') || s.toLowerCase().startsWith('share:'))).join('\\n');
  return txt.replace(/[ \\t]{2,}/g,' ').trim();
}
"""


# --- NUEVO helper JS para leer el panel activo de tabs ---
def _PANEL_TEXT_JS():
    return """
    (info) => {
      const { panelId, hrefHash } = info;

      function isVisible(el) {
        if (!el) return false;
        const style = window.getComputedStyle(el);
        if (style.display === 'none' || style.visibility === 'hidden' || +style.opacity === 0) return false;
        const rect = el.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      }

      // 1) aria-controls
      if (panelId) {
        try {
          const el = document.querySelector("#" + CSS.escape(panelId));
          if (isVisible(el)) return el.innerText || "";
        } catch {}
      }

      // 2) href="#id"
      if (hrefHash) {
        try {
          const el = document.querySelector("#" + CSS.escape(hrefHash));
          if (isVisible(el)) return el.innerText || "";
        } catch {}
      }

      // 3) role="tabpanel" visible
      try {
        const candidates = Array.from(document.querySelectorAll("[role='tabpanel']"));
        for (const el of candidates) {
          const ariaHidden = el.getAttribute("aria-hidden");
          if ((ariaHidden === null || ariaHidden === "false") && isVisible(el)) {
            return el.innerText || "";
          }
        }
      } catch {}

      // 4) clases típicas de activo
      const activeSelectors = [
        ".tab-pane.active",
        ".tab-content .active",
        ".is-active",
        ".current",
        ".selected",
        ".uk-active"
      ];
      for (const sel of activeSelectors) {
        try {
          const el = document.querySelector(sel);
          if (isVisible(el)) return el.innerText || "";
        } catch {}
      }

      return "";
    }
    """

def click_tabs_and_collect(page, url: str, cfg: CrawlConfig) -> Tuple[Dict[str, str], List[str]]:
    sections: Dict[str, str] = {}
    all_links: List[str] = []
    parsed = urlparse(url)
    is_home = (url.rstrip("/") == f"{parsed.scheme}://{parsed.netloc}")

    # selector del contenedor "principal"
    container_sel = detect_main_container_selector(page)

    # Texto base (Main) por defecto
    base_text = get_main_content(page, url, is_home, cfg)
    base_text = clean_section_text(base_text)

    # Si no renderizamos JS o no hay tabs, devolvemos Main + links
    if not cfg.renderJs:
        sections["Main"] = base_text
        all_links = extract_links(page, url, cfg)
        return sections, all_links

    tabs = find_tab_elements(page)

    if not tabs:
        sections["Main"] = base_text
        all_links = extract_links(page, url, cfg)
        return sections, all_links

    # Hay tabs: reconstruimos Main eliminando los paneles/tablists del DOM
    try:
        cleaned_main = page.evaluate(CLEAN_MAIN_WITHOUT_TABS_JS, container_sel) or ""
        cleaned_main = clean_section_text(cleaned_main)
        # si por lo que sea sale muy corto, usa el base
        if len(cleaned_main) >= 40:
            sections["Main"] = cleaned_main
        else:
            sections["Main"] = base_text
    except:
        sections["Main"] = base_text

    seen_texts = {sections["Main"]}
    base_for_similarity = sections["Main"]

    # Links iniciales
    all_links.extend(extract_links(page, url, cfg))

    # Clic de cada tab e intento de leer su panel activo
    for tab in tabs:
        try:
            name = (tab.inner_text() or "").strip() or (tab.get_attribute("aria-controls") or "Tab")
            norm_name = normalize_section_name(name)
            if not norm_name:
                continue

            # A la vista y dispara eventos
            try:
                page.evaluate("(el)=>el.scrollIntoView({block:'center', inline:'center'})", tab)
            except:
                pass
            page.evaluate("""
                (el)=>{
                  el.click();
                  el.dispatchEvent(new MouseEvent('mousedown',{bubbles:true}));
                  el.dispatchEvent(new MouseEvent('mouseup',{bubbles:true}));
                  el.dispatchEvent(new Event('click',{bubbles:true}));
                  el.dispatchEvent(new Event('change',{bubbles:true}));
                }
            """, tab)
            page.wait_for_timeout(450)

            panel_id = (tab.get_attribute("aria-controls") or "").strip() or None
            href = tab.get_attribute("href") or ""
            href_hash = href[1:] if href.startswith("#") and len(href) > 1 else None

            try:
                if panel_id:
                    page.wait_for_selector(f"#{panel_id}", state="visible", timeout=2200)
                elif href_hash:
                    page.wait_for_selector(f"#{href_hash}", state="visible", timeout=2200)
                else:
                    page.wait_for_selector("[role='tabpanel'], .tab-pane.active, .is-active, .current", state="visible", timeout=1800)
            except:
                pass

            # Preferir panel activo
            try:
                panel_txt = page.evaluate(_PANEL_TEXT_JS(), {"panelId": panel_id, "hrefHash": href_hash}) or ""
            except:
                panel_txt = ""

            panel_txt = clean_section_text(panel_txt)

            # Si vacío/corto, leer contenedor visible
            if len(panel_txt) < 40:
                try:
                    panel_txt = page.evaluate(VISIBLE_TEXT_JS, container_sel) or ""
                except:
                    try:
                        panel_txt = page.inner_text(container_sel) or ""
                    except:
                        panel_txt = ""
                panel_txt = clean_section_text(panel_txt)

            # Último recurso
            if len(panel_txt) < 40:
                try:
                    panel_txt = clean_section_text(page.inner_text("body") or "")
                except:
                    pass

            # Evitar duplicados y evitar secciones casi iguales a Main
            if panel_txt:
                sim = _similar(panel_txt, base_for_similarity)
                if sim < 0.80 and panel_txt not in seen_texts:
                    sections[norm_name] = panel_txt
                    seen_texts.add(panel_txt)

            # Enlaces
            for lk in extract_links(page, url, cfg):
                if lk not in all_links:
                    all_links.append(lk)

        except Exception as e:
            print(f"[Tab Error] {url}: {e}")
            continue

    return sections, all_links


# =========================
#   GUARDADO DE ARCHIVOS
# =========================
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

    if save_raw_html:
        html_file = os.path.join(job_dir, f"{safe_name}.html")
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html_snippet)

def save_page_structured(job_dir: str,
                         url: str,
                         title: str,
                         sections: Dict[str, str],
                         links: List[str],
                         html_snippet: str) -> Dict:
    os.makedirs(job_dir, exist_ok=True)
    safe_name = url.replace("https://", "").replace("http://", "").replace("/", "_")

    data = {
        "url": url,
        "title": title,
        "sections": sections,   # texto completo por sección
        "links": links,         # <-- AÑADIDO: guarda también los links
        # Si quisieras, podrías guardar un snippet de HTML aquí:
        # "html_snippet": html_snippet[:2000]
    }

    # JSON por página
    json_file = os.path.join(job_dir, f"{safe_name}.json")
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # CSV por página (una fila por sección)
    csv_file = os.path.join(job_dir, f"{safe_name}.csv")
    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["url", "title", "section", "text", "links_count"])
        for sec_name, sec_text in sections.items():
            txt = (sec_text or "").strip()
            w.writerow([url, title, sec_name, txt, len(links)])

    return data

                             
def save_all_results_md(job_dir: str, all_results: List[Dict]):
    """
    Escribe un único Markdown con todo el contenido del crawl:
    - Índice con títulos/URLs
    - Por cada página: título + URL + secciones (texto completo)
    """
    md_path = os.path.join(job_dir, "all_results.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Crawl report — {len(all_results)} page(s)\n\n")

        # Índice
        f.write("## Index\n\n")
        for i, entry in enumerate(all_results, start=1):
            title = entry.get("title") or entry.get("url")
            url = entry.get("url", "")
            f.write(f"{i}. [{title}]({url})\n")
        f.write("\n---\n\n")

        # Detalle
        for entry in all_results:
            title = entry.get("title") or entry.get("url")
            url = entry.get("url", "")
            f.write(f"## {title}\n\n")
            f.write(f"**URL:** {url}\n\n")

            sections = entry.get("sections") or {}
            if not sections:
                f.write("_No sections extracted._\n\n")
                continue

            for sec_name, sec_text in sections.items():
                if not (sec_text or "").strip():
                    continue
                f.write(f"### {sec_name}\n\n")
                f.write((sec_text or "").strip() + "\n\n")


# =========================
#        CRAWLER
# =========================
def crawl_single(start_url: str, cfg: CrawlConfig, output_root: str):
    visited: set[str] = set()
    to_visit: List[Tuple[str, int]] = [(start_url, 0)]

    # carpeta por job
    stamp = time.strftime("%Y%m%d-%H%M%S")
    job_dir = os.path.join(output_root, f"{cfg.name}-{stamp}")
    os.makedirs(job_dir, exist_ok=True)

    # acumulador global
    all_results: List[Dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            context = browser.new_context(user_agent=cfg.userAgent, extra_http_headers=cfg.headers or {})
            page = context.new_page()
            try:
                page.set_default_navigation_timeout(cfg.timeout)
            except:
                pass

            while to_visit and len(visited) < cfg.maxPages:
                url, depth = to_visit.pop(0)
                if url in visited or depth > cfg.maxDepth:
                    continue

                print(f"[Crawling] {url}")
                ok, err = False, ""
                for _ in range(max(cfg.retries, 1)):
                    try:
                        page.goto(url, timeout=cfg.timeout, wait_until="domcontentloaded")
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

                # pestañas/links
                sections, links_all_states = click_tabs_and_collect(page, url, cfg)

                # Normalizar y agendar siguientes
                next_links, seen = [], set()
                for href in links_all_states:
                    link = normalize_url(url, href, cfg)
                    if link and link not in visited and link not in seen:
                        seen.add(link)
                        next_links.append(link)
                        to_visit.append((link, depth + 1))

                # Guardar por página
                save_page_markdown(job_dir, url, title, sections, html, next_links, cfg.saveRawHtml)
                page_data = save_page_structured(job_dir, url, title, sections, next_links, html)  # <-- incluye "links"
                all_results.append(page_data)

                visited.add(url)
        finally:
            browser.close()

    # Guardar agregados globales
    with open(os.path.join(job_dir, "all_results.json"), "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    with open(os.path.join(job_dir, "all_results.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["url", "title", "section", "text_snippet", "links_count"])
        for entry in all_results:
            for sec_name, sec_text in (entry.get("sections") or {}).items():
                txt = (sec_text or "").strip()
                snippet = (txt[:200] + "...") if len(txt) > 200 else txt
                w.writerow([entry["url"], entry["title"], sec_name, snippet, len(entry.get("links", []))])

    # NUEVO: markdown agregado legible
    save_all_results_md(job_dir, all_results)

    return {"jobDir": job_dir, "pages": len(visited)}



# =========================
#    FASTAPI APP
# =========================
app = FastAPI(title="Crawler API")


FRONTENDS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://frontend-crawler-4j12.vercel.app",  # tu dominio de producción en Vercel
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=FRONTENDS,                 # lista explícita
    allow_origin_regex=r"https://.*\.vercel\.app$",  # previews de Vercel
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


OUTPUT_ROOT = "output_crawler"
JOBS: Dict[str, Dict] = {}

def _init_job(job_id: str, cfg: CrawlConfig, start_url: str):
    JOBS[job_id] = {
        "id": job_id,
        "name": cfg.name or "job",
        "created_at": time.time(),
        "status": {"running": True, "done": False, "error": None},
        "counts": {"crawled": 0},
        "logs": [],
        "jobDir": None,
        "startUrl": start_url,
    }

def _job_log(job_id: str, level: str, message: str):
    job = JOBS.get(job_id)
    if not job:
        return
    job["logs"].append({
        "ts": time.time(),
        "level": level,
        "message": message,
    })

def _run_crawl_job(job_id: str, cfg: CrawlConfig, start_url: str):
    try:
        _job_log(job_id, "info", f"Starting crawl: {start_url}")

        visited: set[str] = set()
        to_visit: List[Tuple[str, int]] = [(start_url, 0)]

        stamp = time.strftime("%Y%m%d-%H%M%S")
        job_dir = os.path.join(OUTPUT_ROOT, f"{cfg.name}-{stamp}")
        os.makedirs(job_dir, exist_ok=True)

        all_results: List[Dict] = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                context = browser.new_context(user_agent=cfg.userAgent, extra_http_headers=cfg.headers or {})
                page = context.new_page()
                try:
                    page.set_default_navigation_timeout(cfg.timeout)
                except:
                    pass

                while to_visit and len(visited) < cfg.maxPages:
                    url, depth = to_visit.pop(0)
                    if url in visited or depth > cfg.maxDepth:
                        continue

                    _job_log(job_id, "info", f"[Crawling] {url}")
                    ok, err = False, ""
                    for _ in range(max(cfg.retries, 1)):
                        try:
                            page.goto(url, timeout=cfg.timeout, wait_until="domcontentloaded")
                            page.wait_for_timeout(300)
                            ok = True
                            break
                        except Exception as e:
                            err = str(e)

                    if not ok:
                        _job_log(job_id, "error", f"[Error] {url}: {err}")
                        continue

                    title = page.title() if cfg.extractTitle else url
                    html = page.content()
                    sections, links_all_states = click_tabs_and_collect(page, url, cfg)

                    next_links, seen = [], set()
                    for href in links_all_states:
                        link = normalize_url(url, href, cfg)
                        if link and link not in visited and link not in seen:
                            seen.add(link)
                            next_links.append(link)
                            to_visit.append((link, depth + 1))

                    save_page_markdown(job_dir, url, title, sections, html, next_links, cfg.saveRawHtml)
                    page_data = save_page_structured(job_dir, url, title, sections, next_links, html)
                    all_results.append(page_data)

                    visited.add(url)
                    JOBS[job_id]["counts"]["crawled"] = len(visited)

            finally:
                browser.close()

        with open(os.path.join(job_dir, "all_results.json"), "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        with open(os.path.join(job_dir, "all_results.csv"), "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["url", "title", "section", "text_snippet", "links_count"])
            for entry in all_results:
                for sec_name, sec_text in (entry.get("sections") or {}).items():
                    txt = (sec_text or "").strip()
                    snippet = (txt[:200] + "...") if len(txt) > 200 else txt
                    w.writerow([entry["url"], entry["title"], sec_name, snippet, len(entry.get("links", []))])

        save_all_results_md(job_dir, all_results)

        JOBS[job_id]["status"]["running"] = False
        JOBS[job_id]["status"]["done"] = True
        JOBS[job_id]["jobDir"] = os.path.basename(job_dir)
        _job_log(job_id, "success", f"Crawl finished. Pages: {len(visited)}")
    except Exception as e:
        JOBS[job_id]["status"]["running"] = False
        JOBS[job_id]["status"]["done"] = False
        JOBS[job_id]["status"]["error"] = str(e)
        _job_log(job_id, "error", f"Job failed: {e}")

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

@app.post("/crawl_async")
@app.post("/crawl_async/")
def start_crawl_async(cfg: CrawlConfig, background: BackgroundTasks):
    if not cfg.startUrls:
        raise HTTPException(status_code=400, detail="startUrls vacío")
    start_url = cfg.startUrls[0]
    job_id = f"{int(time.time()*1000)}"
    _init_job(job_id, cfg, start_url)
    background.add_task(_run_crawl_job, job_id, cfg, start_url)
    return {"job_id": job_id}

@app.get("/jobs/{job_id}")
def job_info(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "id": job["id"],
        "name": job["name"],
        "created_at": job["created_at"],
        "status": job["status"],
        "counts": job["counts"],
        "jobDir": job.get("jobDir"),
        "startUrl": job.get("startUrl"),
    }

@app.get("/jobs/{job_id}/logs")
def job_logs(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"logs": job["logs"]}

@app.get("/download/{job}/{filename}")
def download_file(job: str, filename: str):
    file_path = os.path.join(OUTPUT_ROOT, job, filename)
    if not os.path.exists(file_path):
        return {"error": "File not found"}
    return FileResponse(file_path, filename=filename)

@app.get("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
