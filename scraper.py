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

_reddit_token = {"token": None, "fetched_at": 0}

def get_reddit_token():
    """Get OAuth token using client credentials."""
    if time.time() - _reddit_token["fetched_at"] < 3000:
        return _reddit_token["token"]
    import os
    client_id = os.environ.get("R_CLIENT_ID")
    client_secret = os.environ.get("R_CLIENT_SECRET")
    if not client_id or not client_secret:
        print("[reddit] No credentials found")
        return None
    try:
        r = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(client_id, client_secret),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": "Trentradar/1.0 (cultural trend research tool)"},
            timeout=8
        )
        if r.status_code == 200:
            token = r.json().get("access_token")
            _reddit_token["token"] = token
            _reddit_token["fetched_at"] = time.time()
            print("[reddit] OAuth token obtained")
            return token
        else:
            print("[reddit] Token request failed: {}".format(r.status_code))
    except Exception as e:
        print("[reddit token] {}".format(e))
    return None

def scrape_reddit_hot(subreddit):
    items = []
    try:
        time.sleep(random.uniform(0.3, 0.8))
        token = get_reddit_token()
        if token:
            headers = {
                "Authorization": "Bearer {}".format(token),
                "User-Agent": "Trentradar/1.0 (cultural trend research tool)"
            }
            url = "https://oauth.reddit.com/r/{}/hot?limit=10".format(subreddit)
        else:
            # Fallback without auth
            headers = {"User-Agent": "Trentradar/1.0 (cultural trend research tool)"}
            url = "https://www.reddit.com/r/{}/hot.json?limit=10".format(subreddit)

        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code == 200:
            data = r.json()
            for post in data["data"]["children"]:
                p = post["data"]
                if not p.get("stickied") and p.get("title") and p.get("score", 0) > 20:
                    items.append({
                        "title": p["title"],
                        "url": "https://www.reddit.com" + p["permalink"],
                        "source": "r/{}".format(subreddit),
                        "type": "reddit",
                        "score": p.get("score", 0)
                    })
        else:
            print("[reddit/{}] status {}".format(subreddit, r.status_code))
    except Exception as e:
        print("[reddit/{}] {}".format(subreddit, e))
    return items[:5]

_gtrends_cache = {"data": [], "fetched_at": 0}

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
                ns = {"ht": "https://trends.google.com/trends/trendingsearches/daily"}
                for item in root.findall(".//item")[:12]:
                    title_el = item.find("title")
                    traffic_el = item.find("ht:approx_traffic", ns)
                    title = title_el.text.strip() if title_el is not None and title_el.text else ""
                    traffic = traffic_el.text.strip() if traffic_el is not None and traffic_el.text else ""
                    if title:
                        search_url = "https://trends.google.com/trends/explore?q=" + requests.utils.quote(title) + "&geo=NL"
                        items.append({
                            "title": title + (" ({} searches)".format(traffic) if traffic else ""),
                            "url": search_url,
                            "source": "Google Trends NL",
                            "type": "trends"
                        })
                if items:
                    break
            else:
                print("[google trends] {} status {}".format(trend_url, r.status_code if r else 'None'))
        except Exception as e:
            print("[google trends] {} - {}".format(trend_url, e))
    _gtrends_cache["data"] = items
    _gtrends_cache["fetched_at"] = time.time()
    return items

def scrape_books():
    """Dutch book bestsellers via multiple sources."""
    items = []

    # Try Bruna Bestseller 60 - clean URL without tracking params
    try:
        time.sleep(random.uniform(0.5, 1.0))
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9",
            "Referer": "https://www.google.nl/",
        }
        r = requests.get("https://www.bruna.nl/bestseller/de-bestseller-60", headers=headers, timeout=10)
        print("[bruna] status {}".format(r.status_code))
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            seen = set()
            for selector in ["h2 a", "h3 a", ".product-title a", ".title a",
                            "[class*='title'] a", "[class*='product'] a",
                            "article a", ".card a"]:
                for a in soup.select(selector)[:20]:
                    title = a.get_text(strip=True)
                    href = a.get("href", "")
                    if title and 5 < len(title) < 100 and title not in seen:
                        url = href if href.startswith("http") else "https://www.bruna.nl" + href
                        items.append({"title": title, "url": url, "source": "Bruna Bestseller 60", "type": "books"})
                        seen.add(title)
                if items:
                    break
        print("[bruna] {} items".format(len(items)))
    except Exception as e:
        print("[bruna] {}".format(e))

    # Fallback: Hebban.nl
    if not items:
        try:
            time.sleep(random.uniform(0.5, 1.0))
            r = requests.get("https://www.hebban.nl/boeken/bestsellers", headers=headers, timeout=8)
            print("[hebban] status {}".format(r.status_code))
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                seen = set()
                for selector in ["h2 a", "h3 a", ".book-title a", ".title a",
                                "[class*='book'] a", "[class*='title'] a",
                                "article a", ".card a", ".item a", "li a"]:
                    for a in soup.select(selector)[:15]:
                        title = a.get_text(strip=True)
                        href = a.get("href", "")
                        if title and 5 < len(title) < 100 and title not in seen and "/boek" in href:
                            url = href if href.startswith("http") else "https://www.hebban.nl" + href
                            items.append({"title": title, "url": url, "source": "Hebban.nl", "type": "books"})
                            seen.add(title)
                    if items:
                        break
            print("[hebban] {} items".format(len(items)))
        except Exception as e:
            print("[hebban] {}".format(e))

    return items[:8]

def gather_all(region="nl"):
    """Run all scrapers in parallel with polite delays."""
    all_items = []

    # Human-focused subreddits — personal stories, relationships, lifestyle, culture
    if region == "nl":
        subreddits = [
            "Netherlands",          # Dutch community — local culture and life
            "TrueOffMyChest",       # Raw personal stories
            "relationship_advice",  # Relationships, family, modern love
            "AmItheAsshole",        # Moral dilemmas, family dynamics
            "antiwork",             # Work culture, burnout, career
            "lonely",               # Loneliness, connection
        ]
    else:
        subreddits = [
            "TrueOffMyChest",
            "relationship_advice",
            "AmItheAsshole",
            "antiwork",
            "Millennials",
            "GenZ",
        ]

    scrapers = [
        scrape_nu, scrape_ad, scrape_volkskrant, scrape_parool,
        scrape_libelle, scrape_linda, scrape_rtl, scrape_nos,
        scrape_google_trends_nl, scrape_books
    ]
    scrapers += [lambda s=s: scrape_reddit_hot(s) for s in subreddits]

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
