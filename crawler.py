#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, csv, re, json, time, hashlib, logging, argparse, pathlib
from urllib.parse import urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    import trafilatura
except Exception:
    trafilatura = None
try:
    import fitz
except Exception:
    fitz = None
try:
    from pdfminer.high_level import extract_text as pdfminer_extract_text
except Exception:
    pdfminer_extract_text = None
import requests
from bs4 import BeautifulSoup
from urllib import robotparser
from tqdm import tqdm

def sha1(s: str) -> str: return hashlib.sha1(s.encode('utf-8')).hexdigest()
def sha256(s: str) -> str: return hashlib.sha256(s.encode('utf-8')).hexdigest()
def norm_url(u: str) -> str:
    p = urlparse(u); path = p.path.rstrip('/')
    return urlunparse((p.scheme, p.netloc.lower(), path, '', '', ''))

class RateLimiter:
    def __init__(self, rate_per_sec: float):
        import threading
        self.rate = max(rate_per_sec, 0.1); self.tokens = self.rate
        self.last = time.monotonic(); self.lock = threading.Lock()
    def acquire(self):
        with self.lock:
            while True:
                now = time.monotonic(); elapsed = now - self.last
                self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
                self.last = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0; return
                time.sleep((1.0 - self.tokens) / self.rate)

class RobotsCache:
    def __init__(self, user_agent: str, timeout: int = 15):
        self.user_agent = user_agent; self.timeout = timeout; self.cache = {}
    def can_fetch(self, url: str) -> bool:
        p = urlparse(url); base = f"{p.scheme}://{p.netloc}"
        if base in self.cache:
            rp = self.cache[base]
            if rp is None: return True
            try: return rp.can_fetch(self.user_agent, url)
            except Exception: return True
        robots_url = base + "/robots.txt"; rp = robotparser.RobotFileParser()
        try:
            resp = requests.get(robots_url, timeout=self.timeout, headers={"User-Agent": self.user_agent})
            if resp.status_code != 200 or not resp.content:
                self.cache[base] = None; return True
            rp.parse(resp.text.splitlines()); self.cache[base] = rp
            return rp.can_fetch(self.user_agent, url)
        except Exception:
            self.cache[base] = None; return True

EMAIL_RE = re.compile(r"\b[\w\.-]+@[\w\.-]+\.\w{2,}\b", re.IGNORECASE)
CPF_RE = re.compile(r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b")
CNPJ_RE = re.compile(r"\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b")
MULTISPACE = re.compile(r"[ \t]+"); MULTINEWLINE = re.compile(r"\n{3,}")
def clean_text(s: str) -> str:
    if not s: return ""
    s = s.replace("\ufeff","").replace("-\n","")
    s = MULTISPACE.sub(" ", s); s = MULTINEWLINE.sub("\n\n", s)
    s = EMAIL_RE.sub("<EMAIL>", s); s = CPF_RE.sub("<CPF>", s); s = CNPJ_RE.sub("<CNPJ>", s)
    return s.strip()

def extract_html_main(html_bytes: bytes) -> str:
    if trafilatura is not None:
        try:
            txt = trafilatura.extract(html_bytes, include_comments=False, include_tables=False, favor_recall=True)
            if txt and txt.strip(): return txt.strip()
        except Exception: pass
    try:
        soup = BeautifulSoup(html_bytes, "lxml")
        for tag in soup(["script","style","nav","header","footer","noscript","aside"]): tag.decompose()
        return soup.get_text("\n").strip()
    except Exception: return ""

def extract_pdf_text(pdf_path: str) -> str:
    if fitz is not None:
        try:
            doc = fitz.open(pdf_path)
            out = "\n".join(page.get_text("text") for page in doc)
            if out.strip(): return out.strip()
        except Exception: pass
    if pdfminer_extract_text is not None:
        try: return (pdfminer_extract_text(pdf_path) or "").strip()
        except Exception: pass
    return ""

def build_session(timeout: int):
    default_headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    }
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    s = requests.Session()
    s.headers.update(default_headers)
    retries = Retry(total=3, backoff_factor=0.8, status_forcelist=[429,500,502,503,504])
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    orig = s.request
    def wrapped(method, url, **kwargs):
        kwargs.setdefault("timeout", timeout); return orig(method, url, **kwargs)
    s.request = wrapped; return s

def run(args):
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s: %(message)s")
    log = logging.getLogger("runner_v2")
    UA = args.user_agent
    raw_dir = pathlib.Path(args.outdir) / "data" / "raw"
    text_dir = pathlib.Path(args.outdir) / "data" / "text"
    shards_dir = pathlib.Path(args.outdir) / "data" / "shards"
    logs_dir = pathlib.Path(args.outdir) / "logs"
    for d in (raw_dir, text_dir, shards_dir, logs_dir): d.mkdir(parents=True, exist_ok=True)
    log.info("Início run(v2) | manifest=%s | outdir=%s", args.manifest, args.outdir)
    rows = []
    with open(args.manifest, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            r = {k.strip().lower(): (v or "").strip() for k,v in r.items()}
            if not r.get("url"): continue
            r["url_norm"] = norm_url(r["url"]); rows.append(r)
    seen=set(); dedup_rows=[]
    for r in rows:
        if r["url_norm"] in seen: continue
        seen.add(r["url_norm"]); dedup_rows.append(r)
    session = build_session(args.timeout)
    robots = RobotsCache(user_agent=UA, timeout=args.timeout)
    limiter = RateLimiter(rate_per_sec=args.rate)
    def worker(row):
        url = row["url"]; logging.debug("→ Iniciando: %s", url)
        if not robots.can_fetch(url):
            logging.debug("robots.txt bloqueou: %s", url); return {"url": url, "status": "robots_disallow"}
        limiter.acquire()
        try:
            resp = session.get(url, headers={"User-Agent": UA})
            ct = resp.headers.get("Content-Type",""); content = resp.content; status = resp.status_code
        except Exception as e:
            logging.warning("fetch_error %s: %s", url, e); return {"url": url, "status": f"fetch_error:{e}"}
        if status != 200 or not content:
            logging.debug("http_%s em %s", status, url); return {"url": url, "status": f"http_{status}"}
        is_pdf = ("pdf" in (ct or "").lower()) or url.lower().endswith(".pdf")
        ext = ".pdf" if is_pdf else ".html"; fname = sha256(row["url_norm"])[:20] + ext
        raw_path = raw_dir / fname
        try: raw_path.write_bytes(content)
        except Exception as e:
            logging.warning("save_raw_error %s: %s", url, e); return {"url": url, "status": f"save_raw_error:{e}"}
        try: text = extract_pdf_text(str(raw_path)) if is_pdf else extract_html_main(content)
        except Exception as e:
            logging.warning("extract_error %s: %s", url, e); text = ""
        text = clean_text(text)
        if len(text) < args.min_chars:
            logging.debug("Descartado (curto: %d chars): %s", len(text), url)
            return {"url": url, "status": "too_short", "chars": len(text)}
        tname = fname.replace(ext, ".txt"); text_path = text_dir / tname
        try: text_path.write_text(text, encoding="utf-8")
        except Exception as e:
            logging.warning("save_text_error %s: %s", url, e); return {"url": url, "status": f"save_text_error:{e}"}
        logging.debug("✓ OK: %s (%d chars)", url, len(text))
        return {"url": url, "status": "ok", "text_path": str(text_path), "chars": len(text)}
    results=[]
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs=[ex.submit(worker, r) for r in dedup_rows]
        for fut in tqdm(as_completed(futs), total=len(futs), desc="Coletando/Extraindo"):
            results.append(fut.result())
    ok_items=[r for r in results if r.get("status")=="ok"]
    seen_hash=set(); shard_bytes_limit=int(args.shard_size_mb*1024*1024)
    shard_buf=[]; shard_size=0; shard_id=0
    def flush_shard():
        nonlocal shard_buf, shard_size, shard_id
        if not shard_buf: return
        out_path = shards_dir / f"corpus_{shard_id:04d}.jsonl"
        with out_path.open("w", encoding="utf-8") as f:
            for line in shard_buf: f.write(line + "\n")
        shard_buf=[]; shard_size=0; shard_id+=1; logging.info("Shard salvo: %s", out_path)
    for item in ok_items:
        try: text = pathlib.Path(item["text_path"]).read_text(encoding="utf-8")
        except Exception: continue
        h = sha1(text)
        if h in seen_hash: continue
        seen_hash.add(h)
        line = json.dumps({"text": text}, ensure_ascii=False)
        shard_buf.append(line); shard_size += len(line.encode("utf-8"))
        if shard_size >= shard_bytes_limit: flush_shard()
    flush_shard()
    stats = {
        "total_urls": len(dedup_rows),
        "ok": sum(1 for r in results if r.get("status")=="ok"),
        "too_short": sum(1 for r in results if r.get("status")=="too_short"),
        "http_non200": sum(1 for r in results if str(r.get("status","")).startswith("http_")),
        "errors": [r for r in results if "error" in r.get("status","")],
        "shards": len(list((pathlib.Path(args.outdir)/"data"/"shards").glob("corpus_*.jsonl"))),
    }
    (pathlib.Path(args.outdir)/"logs"/"report.json").write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(stats, indent=2, ensure_ascii=False))

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--outdir", default="corpus_out")
    ap.add_argument("--rate", type=float, default=1.5)
    ap.add_argument("--max-workers", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--min-chars", type=int, default=300)
    ap.add_argument("--shard-size-mb", type=float, default=100.0)
    ap.add_argument("--user-agent", type=str, default="Pesquisa-academica/1.0 (contato: email@example.com)")
    ap.add_argument("--log-level", type=str, default="INFO")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args(); run(args)
