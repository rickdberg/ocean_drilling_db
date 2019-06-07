#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 24 15:50:40 2018

@author: rick

Script to compile Scientific Ocean Drilling databases from the DSDP, ODP, and
IODP (including JR and Chikyu expeditions).

Datasets included:
    Moisture and Density
    Interstitial water chemistry
    Age-depth
    Carbon
    Hole coordinates, water depths, and penetration depths

Does not include Mission-specific platform data. Penetration depths for Chikyu
holes are not included. Age-depth not available for Chikyu.


Output:
    csv files for each dataset
    option to export data into a MySQL database

"""

import metadata
import age_depth
import iw_chem
import mad
import cns

# Option to create a local MySQL database of the data
# Set create_db variable to either True or False
create_db = True



print('Metadata loading...')
hole_metadata = metadata.compile_metadata()
print('Metadata loaded.')

print('Age-depth loading...')
age_depth = age_depth.compile_age_depth()
print('Age-depth loaded.')

print('Pore water loading...')
interstitial_water_chem = iw_chem.compile_iw(hole_metadata)
print('Pore water loaded.')

print('MAD loading...')
mad = mad.compile_mad()
print('MAD loaded.')

print('CNS loading...')
cns = cns.compile_cns()
print('CNS loaded.')

if create_db == True:
    print('Compilation complete, MySQL database and csv files ready.')
else:
    print('Compilation complete, csv files ready.')



# eof
