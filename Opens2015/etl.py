from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

import requests
import sqlite3
from bs4 import BeautifulSoup
import json
from pandas import DataFrame
import pandas as pd
import re
import traceback
import time
import itertools

Base = declarative_base()
class Athlete(Base):
    __tablename__ = 'athlete'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    division = Column(Integer)
    region = Column(Integer)

    def __repr__(self):
        return "<Athlete(name={}, id={})>".format(self.name, self.id)


class Workout(Base):
    __tablename__ = 'workout'
    id = Column(Integer, primary_key=True)
    name = Column(Integer)
    score = Column(String)

    athlete_id = Column(Integer, ForeignKey('athlete.id'))
    # Use cascade='delete,all' to propagate the deletion of a Department onto its Employees
    athlete = relationship(
        Athlete,
        backref=backref('workouts', uselist=True))

    def __repr__(self):
        return "<Workout(name={}, score={})>".format(self.name, self.score)



engine = create_engine('sqlite:///opens2015.db', echo=False)

Session = sessionmaker()
Session.configure(bind=engine)
Base.metadata.create_all(engine)
s = Session()


def get_athletes(params, source):
    division = params['division']
    region = params['region']

    soup = BeautifulSoup(source)
    leaderboard = soup.findAll('table', attrs={"id":"lbtable"})[0]
    participants = leaderboard.findAll('tr',attrs={'class':""})
    d = []
    registered_new_athlete = False
    for participant in participants[1:]:

        position = participant.findAll('td',attrs={'class':"number"})
        if len(participant) == 0:
            continue
        try:
            position = position[0].contents[0]
            athlete = build_athlete(participant)
            athlete.division = division
            athlete.region = region
            register_score(athlete, participant)
            s.add(athlete)
            s.flush()
            s.commit()
            registered_new_athlete = True
        # except FlushError as fe:
        #     print(fe)
        #     print(traceback.format_exc())
        #     continue
        except IntegrityError as ie:
            print("Error in importing {}".format(athlete))
            # print(ie)
            # print(traceback.format_exc())
            s.rollback()
            continue
        except Exception as e:
            print(e.__class__)
            print(e)
            print(traceback.format_exc())
            s.rollback()
            continue
    return registered_new_athlete


def build_athlete(soup):
    name_ = soup.findAll('td',attrs={'class':"name"})[0]
    link = name_.findAll('a')[0].attrs['href']
    athlete_id = link.split('/')[-1]
    athlete_name = name_.findAll('a')[0].contents[0]
    athlete = Athlete(id=int(athlete_id), name=athlete_name)
    return athlete


def register_score(athlete, soup):
    score_cells = soup.findAll('td', attrs={"class":"score-cell"})
    scores = [score_cell.span.contents[0].strip() for score_cell in score_cells]

    for i, score in enumerate(scores):
        wod = Workout(name=i, score=score)
        athlete.workouts.append(wod)


def query(division, region, page):
    website = "http://games.crossfit.com/scores/leaderboard.php"
    params = {
        "stage":0,
        "sort":0,
        "division":division,
        "region": region,
        "numberperpage":100,
        "page":page,
        "competition":0,
        "frontpage":0,
        "expanded":0,
        "full":1,
        "year":15,
        "showtoggles":0,
        "hidedropdowns":0,
        "showathleteac":1,
        "athletename":None,
        "fittest":1,
        "fitSelect":0,
        "scaled":0}
    return website, params


def download():
    divisions = range(1,18)
    regions = range(1,19)
    div_reg = itertools.product(divisions, regions)

    athletes = []
    previous_name = ""
    for division, region in div_reg:
        page = 1

        while True:
            print("Division:{} Region:{} Page:{}".format(division, region, page))
            url, params = query(division, region, page)

            tries = 0
            while tries < 5:
                try:
                    r = requests.get(url, params=params)
                    break
                except Exception as e:
                    # print(e)
                    # print(traceback.format_exc(e))
                    print("Try {}".format(tries))
                    tries += 1
                    time.sleep(2)
                    continue

            if tries == 5:
                print("Tried {} 5 times with no success. You might want to try manually".format(r.url))
                page += 1
                continue

            registered_new_athlete = get_athletes(params, r.content)
            if not registered_new_athlete:
                break

            page += 1


