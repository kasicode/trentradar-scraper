from flask import Flask, jsonify, request
import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.google.nl/",
    "DNT": "1",
}

def polite_get(url, timeout=8):
    """Fetch URL with a small random delay to be a polite scraper."""
    time.sleep(random.uniform(0.5, 1.5))
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        print("[fetch] {} - {}".format(url, e))
        return None

def safe_soup(url, timeout=8):
    r = polite_get(url, timeout)
    if r:
        return BeautifulSoup(r.text, "html.parser")
    return None

# ── Dutch news scrapers ───────────────────────────────────────────────────────

def scrape_nu():
    items = []
    try:
        # NU.nl meest gelezen via their public RSS
        r = polite_get("https://www.nu.nl/rss/Algemeen")
        if r:
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:8]:
                title_el = item.find("title")
                link_el = item.find("link")
                title = (title_el.text or "").strip()
                link = (link_el.text or "").strip()
                if title and len(title) > 15:
                    items.append({"title": title, "url": link, "source": "NU.nl"})
    except Exception as e:
        print("[nu.nl] {}".format(e))

    # Fallback: scrape homepage
    if not items:
        try:
            soup = safe_soup("https://www.nu.nl")
            if soup:
                seen = set()
                for a in soup.select("h2 a, h3 a, .item__title a, [data-tc='article-title'] a")[:20]:
                    title = a.get_text(strip=True)
                    href = a.get("href", "")
                    if title and len(title) > 20 and title not in seen:
                        url = href if href.startswith("http") else "https://www.nu.nl" + href
                        items.append({"title": title, "url": url, "source": "NU.nl"})
                        seen.add(title)
        except Exception as e:
            print("[nu.nl fallback] {}".format(e))
    return items[:6]

def scrape_ad():
    items = []
    # Try RSS first
    try:
        r = polite_get("https://www.ad.nl/rss.xml")
        if r and r.status_code == 200:
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:8]:
                title_el = item.find("title")
                link_el = item.find("link")
                title = (title_el.text or "").strip()
                link = (link_el.text or "").strip()
                if title and len(title) > 15:
                    items.append({"title": title, "url": link, "source": "AD.nl"})
    except Exception as e:
        print("[ad.nl rss] {}".format(e))

    # Fallback: scrape with extra headers
    if not items:
        try:
            headers = dict(HEADERS)
            headers["Referer"] = "https://www.google.nl/search?q=ad+nieuws"
            r = requests.get("https://www.ad.nl", headers=headers, timeout=8)
            soup = BeautifulSoup(r.text, "html.parser")
            seen = set()
            for a in soup.select("h2 a, h3 a, .article__title a, .ankeiler__title a")[:20]:
                title = a.get_text(strip=True)
                href = a.get("href", "")
                if title and len(title) > 20 and title not in seen:
                    url = href if href.startswith("http") else "https://www.ad.nl" + href
                    items.append({"title": title, "url": url, "source": "AD.nl"})
                    seen.add(title)
        except Exception as e:
            print("[ad.nl fallback] {}".format(e))
    return items[:6]

def scrape_volkskrant():
    items = []
    try:
        r = polite_get("https://www.volkskrant.nl/rss.xml")
        if r and r.status_code == 200:
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:8]:
                title_el = item.find("title")
                link_el = item.find("link")
                title = (title_el.text or "").strip()
                link = (link_el.text or "").strip()
                if title and len(title) > 15:
                    items.append({"title": title, "url": link, "source": "de Volkskrant"})
    except Exception as e:
        print("[volkskrant rss] {}".format(e))

    if not items:
        try:
            soup = safe_soup("https://www.volkskrant.nl/meest-gelezen")
            if soup:
                seen = set()
                for a in soup.select("h2 a, h3 a, .teaser__title a, article a")[:20]:
                    title = a.get_text(strip=True)
                    href = a.get("href", "")
                    if title and len(title) > 20 and title not in seen:
                        url = href if href.startswith("http") else "https://www.volkskrant.nl" + href
                        items.append({"title": title, "url": url, "source": "de Volkskrant"})
                        seen.add(title)
        except Exception as e:
            print("[volkskrant fallback] {}".format(e))
    return items[:6]

def scrape_parool():
    items = []
    try:
        r = polite_get("https://www.parool.nl/rss.xml")
        if r and r.status_code == 200:
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:8]:
                title_el = item.find("title")
                link_el = item.find("link")
                title = (title_el.text or "").strip()
                link = (link_el.text or "").strip()
                if title and len(title) > 15:
                    items.append({"title": title, "url": link, "source": "Het Parool"})
    except Exception as e:
        print("[parool rss] {}".format(e))

    if not items:
        try:
            soup = safe_soup("https://www.parool.nl/meest-gelezen")
            if soup:
                seen = set()
                for a in soup.select("h2 a, h3 a, .teaser__title a, article a")[:20]:
                    title = a.get_text(strip=True)
                    href = a.get("href", "")
                    if title and len(title) > 20 and title not in seen:
                        url = href if href.startswith("http") else "https://www.parool.nl" + href
                        items.append({"title": title, "url": url, "source": "Het Parool"})
                        seen.add(title)
        except Exception as e:
            print("[parool fallback] {}".format(e))
    return items[:6]

def scrape_libelle():
    items = []
    try:
        soup = safe_soup("https://www.libelle.nl")
        if soup:
            seen = set()
            for a in soup.select("h2 a, h3 a, .article-title a, .card__title a, [class*='title'] a")[:20]:
                title = a.get_text(strip=True)
                href = a.get("href", "")
                if title and len(title) > 20 and title not in seen:
                    url = href if href.startswith("http") else "https://www.libelle.nl" + href
                    items.append({"title": title, "url": url, "source": "Libelle"})
                    seen.add(title)
    except Exception as e:
        print("[libelle] {}".format(e))
    return items[:6]

def scrape_linda():
    items = []
    try:
        soup = safe_soup("https://www.linda.nl")
        if soup:
            seen = set()
            for a in soup.select("h2 a, h3 a, .article__title a, .card-title a, [class*='title'] a")[:20]:
                title = a.get_text(strip=True)
                href = a.get("href", "")
                if title and len(title) > 20 and title not in seen:
                    url = href if href.startswith("http") else "https://www.linda.nl" + href
                    items.append({"title": title, "url": url, "source": "Linda.nl"})
                    seen.add(title)
    except Exception as e:
        print("[linda] {}".format(e))
    return items[:6]

def scrape_rtl():
    items = []
    try:
        # RTL Nieuws RSS
        r = polite_get("https://www.rtlnieuws.nl/rss.xml")
        if r and r.status_code == 200:
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:8]:
                title_el = item.find("title")
                link_el = item.find("link")
                title = (title_el.text or "").strip()
                link = (link_el.text or "").strip()
                if title and len(title) > 15:
                    items.append({"title": title, "url": link, "source": "RTL Nieuws"})
    except Exception as e:
        print("[rtl rss] {}".format(e))

    if not items:
        try:
            soup = safe_soup("https://www.rtlnieuws.nl")
            if soup:
                seen = set()
                for a in soup.select("h2 a, h3 a, .article-title a, .card__title a")[:20]:
                    title = a.get_text(strip=True)
                    href = a.get("href", "")
                    if title and len(title) > 20 and title not in seen:
                        url = href if href.startswith("http") else "https://www.rtlnieuws.nl" + href
                        items.append({"title": title, "url": url, "source": "RTL Nieuws"})
                        seen.add(title)
        except Exception as e:
            print("[rtl fallback] {}".format(e))
    return items[:6]

def scrape_nos():
    """NOS Nieuws - public broadcaster, good general news signal."""
    items = []
    try:
        r = polite_get("https://feeds.nos.nl/nosnieuwsalgemeen")
        if r and r.status_code == 200:
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:8]:
                title_el = item.find("title")
                link_el = item.find("link")
                title = (title_el.text or "").strip()
                link = (link_el.text or "").strip()
                if title and len(title) > 15:
                    items.append({"title": title, "url": link, "source": "NOS Nieuws"})
    except Exception as e:
        print("[nos] {}".format(e))
    return items[:5]

def scrape_rss_source(url, source_name, limit=5):
    """Generic RSS scraper for DPG and other sources."""
    items = []
    try:
        r = polite_get(url)
        if r and r.status_code == 200:
            root = ET.fromstring(r.content)
            for item in root.findall(".//item")[:limit]:
                title_el = item.find("title")
                link_el = item.find("link")
                title = (title_el.text or "").strip()
                link = (link_el.text or "").strip()
                if title and len(title) > 15:
                    items.append({"title": title, "url": link, "source": source_name})
        else:
            print("[{}] status {}".format(source_name, r.status_code if r else 'None'))
    except Exception as e:
        print("[{}] {}".format(source_name, e))
    return items[:limit]

def zyte_get(url, use_browser=True):
    """Fetch URL via Zyte API — handles JS-rendered pages."""
    import os
    api_key = os.environ.get("ZYTE_API_KEY")
    if not api_key:
        print("[zyte] No ZYTE_API_KEY found")
        return None
    try:
        response = requests.post(
            "https://api.zyte.com/v1/extract",
            auth=(api_key, ""),
            json={
                "url": url,
                "browserHtml": use_browser,
                "httpResponseBody": not use_browser,
            },
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            html = data.get("browserHtml") or data.get("httpResponseBody", "")
            if isinstance(html, bytes):
                html = html.decode("utf-8", errors="ignore")
            return html
        else:
            print("[zyte] {} status {}".format(url, response.status_code))
    except Exception as e:
        print("[zyte] {}".format(e))
    return None

def scrape_zyte(url, source_name, base_url=None):
    """Scrape a meest-gelezen page via Zyte API."""
    items = []
    html = zyte_get(url)
    if not html:
        return items
    try:
        from bs4 import BeautifulSoup as BS
        soup = BS(html, "html.parser")
        seen = set()
        # DPG Media regional sites use these selectors
        selectors = [
            "h2 a", "h3 a",
            ".article__title a",
            ".teaser__title a",
            ".card__title a",
            "[class*='title'] a",
            "[class*='heading'] a",
            "[class*='article'] a",
            "article a",
            "li a",
            "a[href*='/nieuws/']",
            "a[href*='/sport/']",
            "a[href*='/entertainment/']",
            "a[href*='/life/']",
        ]
        for selector in selectors:
            for a in soup.select(selector)[:25]:
                title = a.get_text(strip=True)
                href = a.get("href", "")
                if title and len(title) > 20 and title not in seen:
                    if href.startswith("http"):
                        link = href
                    elif href.startswith("/") and base_url:
                        link = base_url + href
                    else:
                        continue
                    items.append({"title": title, "url": link, "source": source_name})
                    seen.add(title)
            if len(items) >= 4:
                break
        print("[zyte/{}] {} items".format(source_name, len(items)))
        if not items:
            # Log a snippet to help debug selectors
            print("[zyte/{}] HTML snippet: {}".format(source_name, html[1000:1300]))
    except Exception as e:
        print("[zyte/{}] parse error: {}".format(source_name, e))
    return items[:6]

_gtrends_cache = {"data": [], "fetched_at": 0}

def scrape_international_books():
    """International bestsellers as cultural signal — NYT Books RSS."""
    items = []
    feeds = [
        ("https://rss.nytimes.com/services/xml/rss/nyt/Books.xml", "NYT Books"),
        ("https://feeds.feedburner.com/goodreads/YkuY", "Goodreads"),
    ]
    for url, source in feeds:
        try:
            r = requests.get(url, headers=HEADERS, timeout=8)
            if r.status_code == 200:
                root = ET.fromstring(r.content)
                for item in root.findall(".//item")[:6]:
                    title_el = item.find("title")
                    link_el = item.find("link")
                    desc_el = item.find("description")
                    title = (title_el.text or "").strip()
                    link = (link_el.text or "").strip()
                    desc = ""
                    if desc_el is not None and desc_el.text:
                        import re as _re
                        desc = _re.sub(r"<[^>]+>", "", desc_el.text).strip()[:200]
                    if title and len(title) > 5:
                        # Combine title and description for richer signal
                        full_title = title if not desc else "{} — {}".format(title, desc[:100])
                        items.append({
                            "title": full_title,
                            "url": link,
                            "source": source,
                            "type": "books"
                        })
                if items:
                    print("[books/{}] {} items".format(source, len(items)))
                    break
        except Exception as e:
            print("[books/{}] {}".format(source, e))
    return items[:6]

def scrape_google_trends_nl():
    if time.time() - _gtrends_cache["fetched_at"] < 1800:
        return _gtrends_cache["data"]
    items = []
    trend_urls = [
        "https://trends.google.com/trending/rss?geo=NL",
        "https://trends.google.com/trends/trendingsearches/daily/rss?geo=NL",
    ]
    for trend_url in trend_urls:
        try:
            r = polite_get(trend_url)
            if r and r.status_code == 200:
                root = ET.fromstring(r.content)
                ns = {"ht": "https://trends.google.com/trending/rss"}
                for item in root.findall(".//item")[:12]:
                    title_el = item.find("title")
                    traffic_el = item.find("ht:approx_traffic", ns)
                    desc_el = item.find("description")
                    title = title_el.text.strip() if title_el is not None and title_el.text else ""
                    traffic = traffic_el.text.strip() if traffic_el is not None and traffic_el.text else ""

                    # Extract context from related news articles
                    context = ""
                    news_items = item.findall("ht:news_item", ns)
                    if news_items:
                        news_titles = []
                        for ni in news_items[:2]:
                            ni_title = ni.find("ht:news_item_title", ns)
                            ni_source = ni.find("ht:news_item_source", ns)
                            if ni_title is not None and ni_title.text:
                                src = " ({})".format(ni_source.text) if ni_source is not None and ni_source.text else ""
                                news_titles.append(ni_title.text.strip() + src)
                        if news_titles:
                            context = " — " + " / ".join(news_titles)

                    # Fallback: use description if no news items
                    if not context and desc_el is not None and desc_el.text:
                        import re as _re
                        clean = _re.sub(r"<[^>]+>", "", desc_el.text).strip()[:150]
                        if clean and clean.lower() != title.lower():
                            context = " — " + clean

                    if title:
                        search_url = "https://trends.google.com/trends/explore?q=" + requests.utils.quote(title) + "&geo=NL"
                        full_title = title
                        if traffic:
                            full_title += " ({} searches)".format(traffic)
                        if context:
                            full_title += context
                        items.append({
                            "title": full_title,
                            "url": search_url,
                            "source": "Google Trends NL",
                            "type": "trends"
                        })
                if items:
                    print("[google trends] {} items".format(len(items)))
                    break
            else:
                print("[google trends] {} status {}".format(trend_url, r.status_code if r else 'None'))
        except Exception as e:
            print("[google trends] {} - {}".format(trend_url, e))
    _gtrends_cache["data"] = items
    _gtrends_cache["fetched_at"] = time.time()
    return items

def gather_all(region="nl"):
    """Run all scrapers in parallel with polite delays."""
    all_items = []

    scrapers = [
        scrape_nu, scrape_ad, scrape_volkskrant, scrape_parool,
        scrape_libelle, scrape_linda, scrape_rtl, scrape_nos,
        scrape_google_trends_nl, scrape_international_books,
        lambda: scrape_rss_source("https://www.trouw.nl/rss.xml", "Trouw"),
    ]

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fn): fn for fn in scrapers}
        for future in as_completed(futures, timeout=20):
            try:
                result = future.result()
                if result:
                    all_items.extend(result)
            except Exception as e:
                print("[parallel error] {}".format(e))

    return all_items

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def health():
    return jsonify({"status": "ok", "service": "trentradar-scraper"})

@app.route("/scrape", methods=["POST", "GET"])
def scrape():
    body = request.json or {}
    region = body.get("region", "nl")
    items = gather_all(region)
    print("[scrape] Returned {} items".format(len(items)))
    return jsonify({"items": items, "count": len(items)})

if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8081)))
