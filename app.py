import asyncio
import aiohttp
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json, os
import docx2txt
import PyPDF2

app = FastAPI(title="Async Web Scraper API")

visited = set()
data = []

# ---------- Bandwidth Tracker ----------
bandwidth_used = 0
BANDWIDTH_LIMIT = 1_073_741_824  # 1 GB in bytes
WARNING_THRESHOLD = 900 * 1024 * 1024
# On Vercel you can’t persist a file, so keep usage in memory only


def load_usage():
    global bandwidth_used
    return bandwidth_used


def save_usage():
    # No file saving on Vercel
    pass


# ---------- Proxy Loader ----------
def load_proxies(file_path="proxy_list.txt"):
    proxies = []
    if not os.path.exists(file_path):
        return proxies
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            ip, port, user, pwd = line.split(":")
            proxies.append(f"http://{user}:{pwd}@{ip}:{port}")
    return proxies


# ---------- File Readers ----------
async def read_pdf(content):
    text = ""
    try:
        from io import BytesIO
        reader = PyPDF2.PdfReader(BytesIO(content))
        for page in reader.pages:
            text += page.extract_text() or ""
    except:
        pass
    return text


async def read_docx(content):
    text = ""
    try:
        from io import BytesIO
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as tmp:
            tmp.write(content)
            tmp.flush()
            text = docx2txt.process(tmp.name)
        os.remove(tmp.name)
    except:
        pass
    return text


async def read_txt(content):
    try:
        return content.decode("utf-8", errors="ignore")
    except:
        return ""


async def read_xml(content):
    try:
        soup = BeautifulSoup(content.decode("utf-8", errors="ignore"), "xml")
        return soup.get_text(separator=" ", strip=True)
    except:
        return ""


# ---------- Fetch ----------
async def fetch(url, session, proxy=None):
    global bandwidth_used
    try:
        async with session.get(url, proxy=proxy, timeout=15) as resp:
            if resp.status == 200:
                content = await resp.read()
                bandwidth_used += len(content)
                if bandwidth_used >= BANDWIDTH_LIMIT:
                    raise Exception("Bandwidth limit exceeded")
                return content, resp.headers.get("Content-Type", "")
    except:
        return None, None
    return None, None


# ---------- Worker ----------
async def worker(base_url, queue, session, proxy, limit):
    global visited, data
    while True:
        url = await queue.get()

        # Stop if limit reached
        if url in visited or (limit is not None and len(visited) >= limit):
            queue.task_done()
            continue

        visited.add(url)

        content, ctype = await fetch(url, session, proxy)
        text = ""

        if content:
            if url.lower().endswith(".pdf"):
                text = await read_pdf(content)
            elif url.lower().endswith(".docx"):
                text = await read_docx(content)
            elif url.lower().endswith(".txt"):
                text = await read_txt(content)
            elif url.lower().endswith(".xml"):
                text = await read_xml(content)
            elif ctype and "text/html" in ctype:
                html = content.decode("utf-8", errors="ignore")
                soup = BeautifulSoup(html, "html.parser")
                text = soup.get_text(separator=" ", strip=True)

                # Only enqueue new links if limit not reached
                if limit is None or len(visited) < limit:
                    for a in soup.find_all("a", href=True):
                        link = urljoin(url, a["href"])
                        if urlparse(link).netloc == urlparse(base_url).netloc:
                            if link not in visited:
                                await queue.put(link)

        if text.strip():
            data.append({"url": url, "content": text})

        queue.task_done()


# ---------- Scraper ----------
async def scrape_website(base_url, proxies, limit=None):
    global visited, data
    visited = set()
    data = []

    queue = asyncio.Queue()
    await queue.put(base_url)

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        tasks = []
        num_workers = len(proxies) if proxies else 4
        for i in range(num_workers):
            proxy = proxies[i % len(proxies)] if proxies else None
            task = asyncio.create_task(worker(base_url, queue, session, proxy, limit))
            tasks.append(task)

        while not queue.empty() and (limit is None or len(visited) < limit):
            await asyncio.sleep(1)

        await queue.join()

        for t in tasks:
            t.cancel()

    return data


# ---------- FastAPI Endpoints ----------
@app.get("/scrape")
async def scrape_endpoint(
    website_url: str = Query(..., description="Website URL to scrape"),
    number_of_pages: int | None = Query(None, description="Number of pages to scrape"),
    proxy: bool = Query(False, description="Enable proxy (requires proxy_list.txt)")
):
    load_usage()
    proxies = load_proxies("proxy_list.txt") if proxy else []
    result = await scrape_website(website_url, proxies, limit=number_of_pages)

    hostname = urlparse(website_url).netloc

    # Return JSON directly (no file writes on Vercel)
    return JSONResponse(content={
        "website": hostname,
        "pages_scraped": len(result),
        "data": result
    })


@app.get("/bandwidth")
async def bandwidth_status():
    """Return bandwidth usage in MB (used, remaining, total)."""
    used_mb = round(bandwidth_used / 1024 / 1024, 2)
    total_mb = round(BANDWIDTH_LIMIT / 1024 / 1024, 2)
    remaining_mb = round(total_mb - used_mb, 2)
    return JSONResponse(content={
        "used_mb": used_mb,
        "remaining_mb": remaining_mb,
        "total_mb": total_mb
    })
