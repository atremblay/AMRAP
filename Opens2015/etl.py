from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

import requests
import sqlite3
from bs4 import BeautifulSoup
from pandas import DataFrame
import re
import traceback
import time
import itertools
import os

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
    leaderboard = soup.findAll('table', attrs={"id": "lbtable"})[0]
    participants = leaderboard.findAll('tr', attrs={'class': ""})
    registered_new_athlete = False

    num_new_athletes = 0
    for participant in participants[1:]:

        position = participant.findAll('td', attrs={'class': "number"})
        if len(participant) == 0:
            continue
        try:
            position = position[0].contents[0]
            athlete = build_athlete(participant)
            athlete.division = division
            athlete.region = region
            register_score(athlete, participant)
            # print(athlete)
            s.add(athlete)
            s.flush()
            s.commit()
            s.clear()
            registered_new_athlete = True
            num_new_athletes += 1
            # print("added")
        except IntegrityError:
            # print("{} already in the database".format(athlete))
            s.rollback()
            continue
        except Exception as e:
            print(e.__class__)
            print(e)
            print(traceback.format_exc())
            s.rollback()
            continue
    return registered_new_athlete, num_new_athletes


def build_athlete(soup):
    name_ = soup.find('td', attrs={'class': "name"})
    link = name_.find('a').attrs['href']
    athlete_id = link.split('/')[-1]
    athlete_name = name_.find('a').contents[0]
    athlete = Athlete(id=int(athlete_id), name=athlete_name)
    return athlete


def register_score(athlete, soup):
    score_cells = soup.findAll('td', attrs={"class": "score-cell"})
    scores = [score_cell.span.contents[0].strip() for score_cell in score_cells]

    for i, score in enumerate(scores):
        wod = Workout(name=i, score=score)
        athlete.workouts.append(wod)


def query(division, region, page):
    website = "http://games.crossfit.com/scores/leaderboard.php"
    params = {
        "stage": 0,
        "sort": 0,
        "division": division,
        "region": region,
        "numberperpage": 100,
        "page": page,
        "competition": 0,
        "frontpage": 0,
        "expanded": 0,
        "full": 1,
        "year": 15,
        "showtoggles": 0,
        "hidedropdowns": 0,
        "showathleteac": 1,
        "athletename": None,
        "fittest": 1,
        "fitSelect": 0,
        "scaled": 0}
    return website, params


def download():

    # Reversing the order of the division because
    # the masters divisions are also in division 1
    # so doing division 1 first prevent from getting the
    # "real" division of the masters because if the unique clause
    # on insert. Reversing will cause to insert them from the masters
    # division first and they will not be inserted again when division
    # 1 is processed
    divisions = reversed(range(1, 18))
    regions = range(1, 18)
    div_reg = itertools.product(divisions, regions)

    for division, region in div_reg:
        page = 1
        new_athletes = 0
        while True:
            print("Division:{} Region:{} Page:{}".format(
                division, region, page))
            url, params = query(division, region, page)

            tries = 0
            while tries < 5:
                try:
                    r = requests.get(url, params=params, timeout=5)
                    break
                except Exception:
                    print("Try {}".format(tries))
                    tries += 1
                    time.sleep(2)
                    continue

            if tries == 5:
                print("Tried {} 5 times with no success. You might want to try manually".format(r.url))
                page += 1
                continue

            registered_new_athlete, num_new_athletes = get_athletes(
                params, r.content)
            new_athletes += num_new_athletes
            print("{} athletes".format(num_new_athletes))
            if not registered_new_athlete:
                break

            page += 1
        print("Division: {} Region:{} Athletes:{}".format(
            division, region, new_athletes))

regex = re.compile("([0-9]+) \(([0-9]+)\)( \- s)*")


def score(x):
    if x == '-- (--)':
        return None
    try:
        match = regex.match(x)
    except:
        return None
    if match is None:
        return None

    groups = match.groups()
    if len(groups) == 3:
        return int(groups[1])
    raise RuntimeError("{}".format(groups))


def Rx(x):
    if x == '-- (--)':
        return None
    try:
        match = regex.match(x)
    except:
        return None
    if match is None:
        return 0
    groups = match.groups()
    if groups[2] is not None:
        return 0
    return 1


def load_data(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    res = cur.execute("""SELECT athlete.id, athlete.name, athlete.division,
        athlete.region, workout.name as wod, workout.score as score
        FROM athlete JOIN workout on athlete.id=workout.athlete_id""")

    df = DataFrame.from_records(
        res.fetchall(),
        columns=[x[0] for x in res.description])

    pivoted = df.pivot_table(
        columns='wod',
        values='score',
        aggfunc=lambda x: x,
        index=['id', 'name', 'division', 'region'])

    pivoted.reset_index(['name', 'division', 'region'], inplace=True)
    pivoted['15.1'] = pivoted[0].apply(score)
    pivoted['15.1 Rx'] = pivoted[0].apply(Rx)

    pivoted['15.1a'] = pivoted[1].apply(score)
    pivoted['15.1a Rx'] = pivoted[1].apply(Rx)

    pivoted['15.2'] = pivoted[2].apply(score)
    pivoted['15.2 Rx'] = pivoted[2].apply(Rx)

    pivoted['15.3'] = pivoted[3].apply(score)
    pivoted['15.3 Rx'] = pivoted[3].apply(Rx)

    del pivoted[0]
    del pivoted[1]
    del pivoted[2]
    del pivoted[3]
    del pivoted[4]
    del pivoted[5]

    return pivoted

