# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 19:32:29 2017

@author: rickdberg

Script for integrating hole summaries into format for database (summary_all)

"""
import pandas as pd
import numpy as np
import re

from ocean_drilling_db import data_filepaths as dfp

def compile_metadata():

    # Load and create summary files
    dsdp = pd.read_csv(dfp.dsdp_meta, sep="\t", header=0, skiprows=None)
    odp = pd.read_csv(dfp.odp_meta, sep="\t", header=0, skiprows=None, encoding='cp1252')
    iodp = pd.read_csv(dfp.iodp_meta,  sep=",", header=0, skiprows=None)
    chikyu = pd.read_csv(dfp.chikyu_meta, sep=",", header=0, skiprows=None)
    chikyu['hole'] = np.empty(len(chikyu)) * np.nan
    chikyu['pen'] = np.empty(len(chikyu)) * np.nan


    # Zip together and create dictionaries to rename columns
    std_cols = ['leg', 'site', 'hole', 'lat', 'lon', 'water_depth', 'total_penetration']
    dsdp_cols = ['leg', 'site', 'hole', 'latitude', 'longitude', 'water depth(m)', 'total penetration(m)']

    odp_lat_re = re.compile(r'\bLatitude\b')
    odp_lat = filter(odp_lat_re.search, list(odp.columns))
    odp_lon_re = re.compile(r'\bLongtitude\b')
    odp_lon = filter(odp_lon_re.search, list(odp.columns))
    odp_wd_re = re.compile(r'\bWater Depth\b')
    odp_wd = filter(odp_wd_re.search, list(odp.columns))
    odp_pen_re = re.compile(r'.*Total Penetration \(m\).*')
    odp_pen = filter(odp_pen_re.search, list(odp.columns))
    odp_cols = ['Leg', 'Site', 'Hole', list(odp_lat)[0], list(odp_lon)[0], list(odp_wd)[0], list(odp_pen)[0]]

    iodp_cols = ['Exp', 'Site', 'Hole', 'Latitude', 'Longitude', 'Water depth (m)', 'Penetration DSF (m)']
    chikyu_cols = ['EXPNAME','HOLENAME','hole','LAT','LON','WTRDEPTH','pen']

    dsdp_dict = dict(zip(dsdp_cols,std_cols))
    odp_dict = dict(zip(odp_cols,std_cols))
    iodp_dict = dict(zip(iodp_cols,std_cols))
    chikyu_dict = dict(zip(chikyu_cols,std_cols))

    dsdp_db = dsdp.loc[:,dsdp_cols]
    dsdp_db = dsdp_db.rename(columns=dsdp_dict)
    odp_db = odp.loc[:,odp_cols]
    odp_db = odp_db.rename(columns=odp_dict)
    odp_db = odp_db[odp_db['leg'] >= 100].reset_index(drop=True)
    odp_db['water_depth'] = odp_db['water_depth'].str.strip().replace('',np.nan).astype(float)
    iodp_db = iodp.loc[:,iodp_cols]
    iodp_db = iodp_db.rename(columns=iodp_dict)
    chikyu_db = chikyu.loc[:,chikyu_cols]
    chikyu_db = chikyu_db.rename(columns=chikyu_dict)

    # Assign Site and Hole to Chikyu data
    chikyu_db = chikyu_db.iloc[1:,:].reset_index(drop=True)
    for n, hole_id in enumerate(chikyu_db.site):
        chikyu_db.loc[n,'hole'] = hole_id[-1]
        chikyu_db.loc[n,'site'] = hole_id[:-1]

    # Transform coordinates to decimal degrees for ODP and IODP data
    def strip_coords(ser, bad_chars):
        for char in bad_chars:
            ser = ser.str.replace(char, '')
        return ser

    def coord_clean(ser):
        ser = ser.str.strip()
        degs = ser.str.extract(r'([^\s]+)').astype(float)
        decs = ser.str.extract(r'(\s.*)').astype(float)
        decs = decs/60
        dec_deg = degs + decs
        return dec_deg

    def coord_transform(coord_series, bad_chars):
        const = []
        for x, coord in enumerate(coord_series):
            if 'S' in coord:
                const.append(-1)
            elif 'W' in coord:
                const.append(-1)
            else:
                const.append(1)

        coords_stripped = strip_coords(coord_series, bad_chars)
        coords = coord_clean(coords_stripped)
        coords = coords.multiply(pd.Series(const), axis=0)
        return coords


    bad_chars = ['°',"'",'N','S','E','W']

    odp_db['lat'] = coord_transform(odp_db['lat'], bad_chars)
    odp_db['lon'] = coord_transform(odp_db['lon'], bad_chars)

    iodp_db['lat'] = coord_transform(iodp_db['lat'], bad_chars)
    iodp_db['lon'] = coord_transform(iodp_db['lon'], bad_chars)

    # Combine all data
    hole_metadata = pd.concat([dsdp_db, odp_db, iodp_db, chikyu_db], axis=0)
    hole_metadata = hole_metadata.reset_index(drop=True)

    # Make specific hole changes
    hole_metadata.loc[hole_metadata['leg'] == '335', ['site','hole']] = [1256, 'D']
    hole_metadata.loc[hole_metadata['leg'] == 302, 'site'] = 'M0004'
    hole_metadata.loc[(hole_metadata['leg'] == 302) &
                      (hole_metadata['water_depth'] == 1225), 'site'] = 'M0001'
    hole_metadata.loc[(hole_metadata['leg'] == 302) &
                      (hole_metadata['water_depth'] == 1211), 'site'] = 'M0002'
    hole_metadata.loc[(hole_metadata['leg'] == 302) &
                      (hole_metadata['water_depth'] == 1205), 'site'] = 'M0003'
    hole_metadata.loc[hole_metadata['site'] == 'U395', 'site'] = 395
    hole_metadata.loc[hole_metadata['site'] == 'U858', 'site'] = 858

    # Assign data types
    hole_metadata[['leg','site','hole']] = hole_metadata[['leg','site','hole']].astype(str)
    hole_metadata[['lat','lon','water_depth','total_penetration']] = hole_metadata[['lat','lon','water_depth','total_penetration']].astype(float)

    # Add hole and site keys
    hole_keys = hole_metadata[['site', 'hole']].drop_duplicates().reset_index(drop=True).reset_index().rename(columns={'index':'hole_key'})
    hole_metadata = hole_metadata.merge(hole_keys, how='left', on = ['site', 'hole'])

    site_keys = hole_metadata['site'].drop_duplicates().reset_index(drop=True).reset_index().rename(columns={'index':'site_key'})
    hole_metadata = hole_metadata.merge(site_keys, how='left', on = ['site'])
    hole_metadata = hole_metadata.loc[:,['hole_key', 'site_key', 'leg', 'site', 'hole', 'lat', 'lon', 'water_depth', 'total_penetration']].reset_index(drop=True)

    # Make site_metadata table
    # site_metadata = hole_metadata[['site_key', 'leg', 'site', 'advection_rate', 'bottom_water_temp', 'temp_gradient']].copy()
    # site_metadata = hole_metadata.drop_duplicates().reset_index(drop=True)

    # Save csvs and send to database
    hole_metadata.to_csv("hole_metadata.csv", sep='\t')
    # site_metadata.to_csv("site_metadata.csv", sep='\t')

    return hole_metadata

# eof
