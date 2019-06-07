#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 17:17:03 2018

@author: rick

User-specific filepaths and variables for database integration


Data must be downloaded from online ocean drilling repositories.
Instructions for downloading as of 10/12/2018:



Notes on data sources:


"""

import os

dir_name = os.path.dirname(__file__)

# DSDP data locations
dsdp_meta = os.path.join('data','dsdp','metadata','sitesum_dsdp.txt')
dsdp_mad = os.path.join('data','dsdp','mad','mad_dsdp.txt')
dsdp_iw = os.path.join('data','dsdp','iw', 'IW_DSDP.txt')
dsdp_age_depth = os.path.join('data','dsdp','age_depth','age_dsdp.txt')
dsdp_carbon = os.path.join('data','dsdp','cns','carbon_dsdp.txt')

# ODP data locations
odp_meta = os.path.join('data','odp','metadata','holedetails_odp.txt')
odp_mad = os.path.join('data','odp','mad','mad_odp.txt')
odp_iw = os.path.join('data','odp','iw', 'iw_odp.txt')
odp_age_depth = os.path.join('data','odp','age_depth','age_depth_odp.txt')
odp_age_profile = os.path.join('data','odp','age_depth','age_profiles_odp.txt')
odp_carbon = os.path.join('data','odp','cns','carbon_odp.txt')

# IODP data locations
iodp_meta = os.path.join('data','iodp','metadata','hole_summary_iodp.csv')
iodp_mad = os.path.join('data','iodp','mad','mad_iodp.csv')
iodp_iw = os.path.join('data','iodp','iw','iw_iodp.csv')
iodp_age_depth = os.path.join('data','iodp','age_depth')
iodp_carbon = os.path.join('data','iodp','cns','carbon_iodp.csv')

# Chikyu data locations
chikyu_meta = os.path.join('data','chikyu','metadata','metadata4jamstec-data-portal.csv')
chikyu_mad = os.path.join('data','chikyu','mad')
chikyu_iw = os.path.join('data','chikyu','iw')
chikyu_carbon = os.path.join('data','chikyu','cns')



