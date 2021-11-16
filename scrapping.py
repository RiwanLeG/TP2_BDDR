from bs4 import BeautifulSoup as soup
import requests
import json
import urllib.parse

def getCreatureName(url):
    page = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    pageSoup = soup(page.content, 'html.parser')
    table = pageSoup.find("table", {"id": "ctl00_MainContent_GridView6"})
    monsters = []
    for tr in table.findAll('tr'):
        for a in tr.find_all('a', href=True):
            if a.text :
                if a['href'][0:10] != "javascript" :
                    content = a['href']
                    monsterName = content.split("ItemName=", 1)[1]
                    monsters.append(getSpells(monsterName))
    return monsters

def getSpells(monsterName):
    monster = {}
    monster["name"] = monsterName
    spells = []

    url = "https://aonprd.com/MonsterDisplay.aspx?ItemName=" + urllib.parse.quote_plus(monsterName)
    print(url)
    page = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    pageSoup = soup(page.content, 'html.parser')

    tag = pageSoup.find(text='Spell-Like Abilities')
    if tag != None:
        nextSiblings = tag.parent.find_next_siblings()
        for s in nextSiblings:
            if s.name == "h3":
                break
            elif s.name == "i":
                if len(s.contents) != 0:
                    spells.append(s.contents[0])

    specialAbilities = pageSoup.find(lambda tag:tag.name=="h3" and "Special Abilities" in tag.text)
    if specialAbilities != None:
        nextSiblings = specialAbilities.find_next_siblings()
        for tag in nextSiblings:
            if tag.name == "h3" or tag.name == "h2" or tag.name == "h1":
                break
            elif tag.name == "b":
                if len(tag.contents) != 0:
                    # print(tag.contents[0])
                    if tag.contents[0].name == "i":
                        spell = tag.contents[0].contents[0].split("(", 1)[0]
                    elif tag.contents[0].name == "br":
                        continue
                    else:
                        spell = tag.contents[0].split("(", 1)[0]
                    spells.append(spell)

    monster["spells"] = spells
    return monster

getSpells("Man-Eating Aurochs")
creatures = getCreatureName("https://aonprd.com/Monsters.aspx?Letter=All")

with open("monsters.json", "w") as jsonFile:
    json.dump(creatures, jsonFile, indent = 4)
