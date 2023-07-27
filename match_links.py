from bs4 import BeautifulSoup
import requests as re
from pymongo import MongoClient, DESCENDING
from datetime import datetime
from multiprocessing import Pool, Event

conn = MongoClient("localhost", 27017)
db = conn["val-db"]
links_collection = db.links

base_link = "https://www.vlr.gg/matches/results/?page="

last_visited_link = list(links_collection.find(
        {"visited":1},
        sort= [("visited_at",1)],
        limit=1
    ))
last_visited_link = last_visited_link[0]["url"].strip("\n") if len(last_visited_link) != 0 else None

def get_max_page():
    req = re.get(base_link + str(1))
    soup = BeautifulSoup(req.content, 'lxml')
    page_nav = soup.find(attrs={"class":"action-container"})
    return max([int(i) for i in page_nav.strings if i.isnumeric()])
    
def scrape_page(n):
    out = []
    req = re.get(base_link + str(n))
    if req.status_code == 200:
        soup = BeautifulSoup(req.content, 'lxml')
        tables_html = soup.find_all(attrs={"class":"wf-card"})
        tables_html = tables_html[1:]
        for table in tables_html:
            links_html = table.find_all("a")

            for link_html in links_html:
                out.append(link_html["href"])
        return out

if __name__ == "__main__":
    max_page = get_max_page()
    pool = Pool(5)
    links_res = pool.imap(scrape_page, range(1,max_page+1))
    out = []
    for link_arr in links_res:
        if last_visited_link in link_arr:
            out.append(link_arr[:link_arr.index(last_visited_link)])
            pool.terminate()
            break
        else:
            out.append(link_arr)
    links = [{"url":item, "visited":0, "visited_at": None} for sublist in out for item in sublist]
    print(f"{len(links)} new links found.")
    links_collection.insert_many(links)
