import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import threading
import time

BASE_URL = 'https://en.wikipedia.org'
START_CATEGORY = '/wiki/Category:Video_games'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; VideoGameScraper/1.0; +https://example.com)'
}

visited_pages = set()
video_games = []
lock = threading.Lock()

def is_valid_wikipedia_url(url):
    if not url.startswith('/wiki/'):
        return False
    # Ignore URLs with colons (usually meta pages like Category:, File:, Help:)
    if ':' in url:
        return False
    return True

def parse_game_page(url):
    full_url = urljoin(BASE_URL, url)
    if full_url in visited_pages:
        return None
    
    with lock:
        visited_pages.add(full_url)

    try:
        r = requests.get(full_url, headers=HEADERS, timeout=5)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, 'html.parser')

        # Get the title
        title = soup.find('h1', {'id': 'firstHeading'}).text.strip()

        # Extract info box (if exists)
        infobox = soup.find('table', {'class': 'infobox vevent'})
        info = {}
        if infobox:
            for row in infobox.find_all('tr'):
                header = row.find('th')
                value = row.find('td')
                if header and value:
                    key = header.text.strip()
                    val = value.text.strip().replace('\n', ' ')
                    info[key] = val
        
        # Collect categories (usually at the bottom in id="mw-normal-catlinks")
        cat_div = soup.find('div', {'id': 'mw-normal-catlinks'})
        categories = []
        if cat_div:
            cat_links = cat_div.find_all('a')[1:]  # skip first "Categories" link
            categories = [a.text.strip() for a in cat_links]

        game_data = {
            'url': full_url,
            'title': title,
            'infobox': info,
            'categories': categories
        }
        return game_data
    except Exception as e:
        print(f'Error parsing {full_url}: {e}')
        return None

def parse_category_page(url):
    full_url = urljoin(BASE_URL, url)
    if full_url in visited_pages:
        return []
    with lock:
        visited_pages.add(full_url)

    try:
        r = requests.get(full_url, headers=HEADERS, timeout=5)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, 'html.parser')

        links_to_scrape = []

        # Pages listed in category (div id mw-pages)
        mw_pages = soup.find('div', {'id': 'mw-pages'})
        if mw_pages:
            for a in mw_pages.find_all('a', href=True):
                href = a['href']
                if is_valid_wikipedia_url(href):
                    links_to_scrape.append(href)

        # Subcategories (div id mw-subcategories)
        mw_subcats = soup.find('div', {'id': 'mw-subcategories'})
        if mw_subcats:
            for a in mw_subcats.find_all('a', href=True):
                href = a['href']
                if href.startswith('/wiki/Category:'):
                    links_to_scrape.append(href)

        # Next page link for category pages
        next_link = soup.find('a', string='next page')
        if next_link and 'href' in next_link.attrs:
            next_page_href = next_link['href']
            links_to_scrape.append(next_page_href)

        return links_to_scrape

    except Exception as e:
        print(f'Error parsing category {full_url}: {e}')
        return []

def worker(start_url):
    queue = [start_url]

    while queue and len(video_games) < 3000:  # Limit total to 3000 for demo, change as needed
        url = queue.pop(0)
        if url in visited_pages:
            continue

        if url.startswith('/wiki/Category:'):
            # Parse category page
            links = parse_category_page(url)
            queue.extend([l for l in links if l not in visited_pages])
        else:
            # Parse game/article page
            data = parse_game_page(url)
            if data:
                with lock:
                    video_games.append(data)
                    if len(video_games) % 300 == 0:
                        # Dump every 300 items
                        filename = f'video_games_{len(video_games)}.json'
                        with open(filename, 'w', encoding='utf-8') as f:
                            json.dump(video_games[-300:], f, ensure_ascii=False, indent=2)
                        print(f'Saved {filename}')

def main():
    t = threading.Thread(target=worker, args=(START_CATEGORY,))
    t.start()
    t.join()

    # Final save
    with open('video_games_final.json', 'w', encoding='utf-8') as f:
        json.dump(video_games, f, ensure_ascii=False, indent=2)
    print(f'Final save done: {len(video_games)} items')

if __name__ == '__main__':
    main()
