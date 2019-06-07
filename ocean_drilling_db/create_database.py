#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 24 15:01:56 2018

@author: rick
"""

from sqlalchemy import create_engine


def create_db(username, password, host, db_name, hole_metadata, age_depth, interstitial_water_chem, mad, cns):
    host_engine = create_engine('mysql://{}:{}@{}'.format(username, password, host)) # connect to server
    host_engine.execute("CREATE DATABASE IF NOT EXISTS {}".format(db_name)) #create db
    engine = create_engine("mysql://{}:{}@{}/{}".format(username, password, host, db_name))

    # Send hole metadata table to database
    hole_metadata.to_sql(name='hole_metadata', con=engine, if_exists='replace', chunksize=3000, index=False)

    # Send Age-Depth table to database
    age_depth.to_sql('age_depth', con=engine, if_exists='replace', chunksize=3000, index=False)

    # Send Interstitial water data to database
    interstitial_water_chem.to_sql('iw_chem', con=engine, if_exists='replace', chunksize=3000, index=False)

    # Send Moisture and Density data to database
    mad.to_sql('mad', con=engine, if_exists='replace', chunksize=3000, index=False)

    # Send Carbon data to database
    cns.to_sql('cns', con=engine, if_exists='replace', chunksize=3000, index=False)

# eof