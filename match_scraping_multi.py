from multiprocessing import Pool
import requests 
import re 
import os
import copy
from bs4 import BeautifulSoup
import pprint
import datetime
import traceback
from pymongo import MongoClient

# base url 
base_url = "https://www.vlr.gg"

# template side stats
blank_side_stats = {
    "side":'',
    "rating":'',
    "ACS":'',
    "kills":'',
    "deaths":'',
    "assists":'',
    "KAST":'',
    "ADR":'',
    "HS%":'',
    "FK":'',
    "FD":''
}

# required stat names and column number
stat_names = {
            "rating":0,
            "ACS":1,
            "kills":2,
            "deaths":3,
            "assists":4,
            "KAST":6,
            "ADR":7,
            "HS%":8,
            "FK":9,
            "FD":10
        }

# template stat names
blank_player_stats = {
    "player":'',
    "agent":'',
    "team":'',
    "side": []
}

# tag regex 
tag = re.compile("<.*>")

def chunk(iterable, chunk_size):
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i:i+chunk_size]

def team_stats_from_board(board, team:int):
    """
        Gets the needed information for each player on a scoreboard
        inputs:
            board - a team's board html from a BeautifulSoup object
            team - number of the team whose scoreboard is provided
    """

    # instantiate list to add players to
    team_stats = []

    # find all table rows within the table body -> returns a list of all players
    players = board.find("tbody").findAll("tr")

    for pn, player in enumerate(players):
        # create deep copies of each template 
        player_stats = copy.deepcopy(blank_player_stats)
        attack_stats = copy.deepcopy(blank_side_stats)
        attack_stats["side"] = 'attack'
        defense_stats = copy.deepcopy(blank_side_stats)
        defense_stats["side"] = 'defense'
        all_stats = copy.deepcopy(blank_side_stats)
        all_stats["side"] = 'all'

        # assign team number
        player_stats["team"] = team

        # player name
        player_stats["player"] = player.find(attrs={"class":"mod-player"}).find("a").findAll("div")[0].string.strip()
        
        # agent
        agent_pic = str(player.find(attrs={"class":"mod-agents"}).find("img")["src"])
        player_stats["agent"] = agent_pic.split("/")[-1][:-4]
        del agent_pic

        # stats
        stats = player.findAll(attrs={"class":"mod-stat"})

        # for each stat in stat_names, get all sides and add them to each side
        for key, n in stat_names.items():
            attack_stats[key], defense_stats[key], all_stats[key] = get_all_sides(stats[n])

        # gather data together
        player_stats["side"] = [attack_stats, defense_stats, all_stats]
        team_stats.append(player_stats)

    return team_stats

def get_all_sides(td):
    """
        function to return all "sides" of a player's stats
        input:
            td - table division html from a BeautifulSoup class
    """

    # get each side and filter any unwanted characters that may cause issues
    attack = ''.join(c for c in td.find(attrs={"class":re.compile("mod-t")}).string.strip() if c not in [",", "%"])
    defense = ''.join(c for c in td.find(attrs={"class":re.compile("mod-ct")}).string.strip() if c not in [",", "%"])
    all = ''.join(c for c in td.find(attrs={"class":re.compile("mod-both")}).string.strip() if c not in [",", "%"])

    return attack, defense, all

def scrape_match(url_record_list):
    thread_conn = MongoClient("localhost", 27017)
    thread_db = thread_conn["val-db"]

    out = []
    for url_record in url_record_list:
        # try statement helps identify which url caused an error, prints the stack trace and saves the html for inspection
        try:

            #create container for match information
            match_info = {}
            games = []

            # assemble url
            comp_url = base_url + url_record["url"]

            # get html from URL
            req = requests.get(comp_url)

            # if successful...
            if req.status_code == 200:

                # create BS object
                soup = BeautifulSoup(req.content, 'lxml')

                # if there is no data available for the match, skip to the next match
                if soup.find(string=re.compile("No data available for this match")) is not None:
                    out.append(False)
                
                # useful information is contained within a div with the class "col mod-3"
                match_info_col = soup.find(attrs={"class":"col mod-3"})

                # get the date the match took place
                date = match_info_col.find(attrs={"class":"match-header-date"}).find(attrs={"class":"moment-tz-convert"})
                match_info["date"] = date["data-utc-ts"] if date is not None else ''

                # get the patch number
                patch = match_info_col.find(string=re.compile("Patch"))
                match_info["patch"] = patch.string.strip().split(" ")[-1] if patch is not None else ''

                # find both teams' names
                names = match_info_col.findAll(attrs={"class":"wf-title-med"})

                # divs where names are contained sometimes have child divs, .string can't be used if they do
                # to solve this, try .string first
                # if it doesnt work, split the html of the div by html tags and take the first value
                try:
                    match_info["team_1_name"] = names[0].string.strip()
                except:
                    strings = [''.join([c for c in item if (c != " " and c != '\n')]) for item in tag.split(str(names[0]))]
                    strings = list(filter(None, strings))
                    match_info["team_1_name"] = strings[0].strip()
                
                try:
                    match_info["team_2_name"] = names[1].string.strip()
                except:
                    strings = [''.join([c for c in item if (c != " " and c != '\n')]) for item in tag.split(str(names[1]))]
                    strings = list(filter(None, strings))
                    match_info["team_2_name"] = strings[0].strip()

                # insert match_info into db
                match_entry = thread_db.matches.insert_one(match_info)

                # get the area containing the player stats
                match_stats = match_info_col.find(attrs={"class":"vm-stats-container"}).findAll(attrs={"data-game-id":True})

                # each match can have multiple games, which need to be recorded individually
                for item in match_stats:

                    # if theres an error reading a value here, its probably because the value doesnt exist (i.e. early matches did not record all information)
                    # in this case, just skip the game
                    try:
                        # skip if:
                        # 1. the game had "TBD", so it has not been played out and likely will never
                        # 2. the game-stats being looked at are the aggregate stats for all games (this can be worked out if needed from the game information)
                        if item["data-game-id"] == "all" or item.find(string=re.compile("TBD")):
                            continue

                        game_info = {}

                        # game score and map
                        header = item.find(attrs={"class":"vm-stats-game-header"})

                        # sometimes the div with the map name in has child tags, try .string and split by html tag if that doesnt work
                        try:
                            game_info["map"] = header.find(attrs={"class":"map"}).find("span").string.strip()
                        except:
                            strings = [''.join([c for c in item if (c != " " and c != '\n')]) for item in tag.split(str(header.find(attrs={"class":"map"})))]
                            strings = list(filter(None, strings))
                            game_info["map"] = strings[0].strip()

                        # team scores
                        game_info["team_1_score"] = int(header.find(attrs={"class":"team"}).find(attrs={"class":"score"}).string.strip())
                        game_info["team_2_score"] = int(header.find(attrs={"class":"team mod-right"}).find(attrs={"class":"score"}).string.strip())

                        # insert game info into db
                        game_info["match"] = match_entry.inserted_id
                        game_entry = thread_db.games.insert_one(game_info)

                        # players and performance
                        boards = item.findAll("table")

                        # team 1 players
                        for player in team_stats_from_board(boards[0], 1):
                            player["game"] = game_entry.inserted_id
                            thread_db.players.insert_one(player)
                        
                        # team 2 players
                        for player in team_stats_from_board(boards[1], 2):
                            player["game"] = game_entry.inserted_id
                            thread_db.players.insert_one(player)

                    except:
                        continue
                    thread_db.links.update_one({"_id": url_record["_id"]}, {"$set": {"visited": 1, "visited_at": datetime.datetime.now()}})
                    out.append(True)

        except:
            traceback.print_exc()
            with open("error.txt", "w", encoding="utf-8") as e_f:
                e_f.write(url_record["url"])
                e_f.write("Reason: \n" + traceback.format_exception() + "\n\n")
            out.append(False)
    return out


if __name__ == "__main__":
    start = datetime.datetime.now()

    main_conn = MongoClient("localhost", 27017)
    main_db = main_conn["val-db"]
    urls = list(main_db.links.find({"visited":0}, projection={"url":True}))

    downloader_pool = Pool(6)
    results = downloader_pool.map(scrape_match, chunk(urls, 1000))

    print(f"Couldn't resolve {results.count(False)} matches.")

    end = datetime.datetime.now()
    print(f"\nFinished at {end.strftime('%H:%M:%S')} after {str(end-start)}")




