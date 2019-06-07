#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 13 14:18:46 2018

@author: rick

Module with functions for importing and integrating moisture and density
data from DSDP, ODP, and IODP (including Chikyu) into a single, standardized csv file.

Functions:

"""
import os
import pandas as pd
import numpy as np

from ocean_drilling_db import data_filepaths as dfp

def load_dsdp_cns():
    # Read in data and rename columns
    dsdp_data = pd.read_csv(dfp.dsdp_carbon, sep="\t", header=0,
                            skiprows=None, encoding='windows-1252')
    dsdp_data = dsdp_data.rename(columns={'sample depth (m)':'sample_depth',
                              'percent total carbon':'total_carbon',
                              'percent organic carbon':'organic_carbon',
                              'percent calcium carbonate (CaCO3)':'calcium_carbonate',
                              'method code':'method','data source code':'data_source'})
    dsdp_data = dsdp_data[['leg','site','hole','core','section','sample_depth',
                           'total_carbon','organic_carbon','calcium_carbonate',
                           'method','data_source']]
    for col in dsdp_data:
        if dsdp_data[col].dtype == object:
            dsdp_data[col] = dsdp_data[col].str.strip()
    dsdp_data = dsdp_data.replace('', np.nan)
    return dsdp_data


def load_odp_cns():
    # Read in data and rename columns
    odp_data = pd.read_csv(dfp.odp_carbon, sep="\t", header=0,
                            skiprows=None, encoding='windows-1252', low_memory=False)
    odp_data = odp_data.rename(columns={'Leg':'leg',
                                        'Site':'site',
                                        'H':'hole',
                                        'Cor':'core',
                                        'Sc':'section',
                                        'Depth (mbsf)':'sample_depth',
                                        'INOR_C (wt %)':'inorganic_carbon',
                                        'CaCO3 (wt %)':'calcium_carbonate',
                                        'TOT_C (wt %)':'total_carbon',
                                        'ORG_C (wt %)':'organic_carbon',
                                        'N (wt %)':'nitrogen',
                                        'S (wt %)':'sulfur',
                                        'H (mg HC/g)':'hydrogen'})
    odp_data = odp_data[['leg','site','hole','core','section','sample_depth',
                           'inorganic_carbon','calcium_carbonate','total_carbon',
                           'organic_carbon','nitrogen','sulfur']]
    for col in odp_data:
        if odp_data[col].dtype == object:
            odp_data[col] = odp_data[col].str.strip()
    odp_data = odp_data.replace('', np.nan)
    return odp_data

def load_iodp_cns():
    # Read in data and rename columns
    iodp_data = pd.read_csv(dfp.iodp_carbon, sep=",", header=0,
                            skiprows=None, encoding='windows-1252')
    iodp_data = iodp_data.replace(to_replace=['nd', 'n.d.', 'ND', 'N.D.',
                                                          'bdl', 'BLD', 'bld', 'bd',
                                                          'BD', 'BDL', 'b.d.l.', 'B.D.L.'], value=0)

    iodp_data = iodp_data.rename(columns={'Exp':'leg',
                                          'Site':'site',
                                          'Hole':'hole',
                                          'Core':'core',
                                          'Sect':'section',
                                          'Top depth CSF-A (m)':'sample_depth',
                                          'Inorganic carbon (wt%)':'inorganic_carbon',
                                          'Calcium carbonate (wt%)':'calcium_carbonate',
                                          'Total carbon (wt%)':'total_carbon',
                                          'Hydrogen (wt%)':'hydrogen',
                                          'Nitrogen (wt%)':'nitrogen',
                                          'Sulfur (wt%)':'sulfur',
                                          'Organic carbon (wt%) by difference (CHNS-COUL)':'organic_carbon',
                                          'Organic carbon (wt%), CHNS with treated sample (wt%)':'organic_carbon_treated',
                                          'Sample treatment method (CHNS organic carbon)':'method',
                                          'Comments':'comments'})
    iodp_data = iodp_data[['leg','site','hole','core','section','sample_depth',
                           'inorganic_carbon','calcium_carbonate','total_carbon',
                           'nitrogen','sulfur','organic_carbon',
                           'organic_carbon_treated','method','comments']]
    for col in iodp_data:
        if iodp_data[col].dtype == object:
            iodp_data[col] = iodp_data[col].str.strip()
    iodp_data = iodp_data.replace('', np.nan)
    iodp_data = iodp_data[iodp_data['leg'] != 'TEST(344)']
    return iodp_data

def load_chikyu_cns():
    chikyu_data = pd.DataFrame()
    ##### File group info #####
    summary = pd.read_csv(dfp.chikyu_meta, sep=",", header=0, skiprows=None)
    summary = summary.iloc[1:,:].reset_index(drop=True)
    summary['leg'] = summary['EXPNAME']
    for n, hole_id in enumerate(summary.HOLENAME):
        summary.loc[n,'hole'] = hole_id[-1]
        summary.loc[n,'site'] = hole_id[:-1]

    for filename in os.listdir(dfp.chikyu_carbon):
        if filename.endswith(".csv"):
            file_path = os.path.join('data','chikyu','cns',filename)
            data_add = pd.read_csv(file_path, sep=",", header=0, skiprows=None)
            # Find leg, site, hole
            for col in data_add.select_dtypes([np.object]):
                for n in np.arange(len(summary)):
                    hole_ids = (summary['site'].map(str) + summary['hole'])
                    if data_add[col].str.contains(hole_ids[n]).any():
                        leg = summary.loc[n,'leg']
                        site = summary.loc[n,'site']
                        hole = summary.loc[n,'hole']
            #Add in leg, site, hole, sample_depth
            leglist = [leg] * len(data_add)
            leglist = pd.Series(leglist, name='leg')
            sitelist = [site] * len(data_add)
            sitelist = pd.Series(sitelist, name='site')
            holelist = [hole] * len(data_add)
            holelist = pd.Series(holelist, name='hole')
            sample_depth = (data_add['Top Depth DSF, MSF, WSF and CSF-A [m]'] + data_add['Bottom Depth DSF, MSF, WSF and CSF-A [m]'])/2
            sample_depth.name = 'sample_depth'
            data = pd.concat([leglist, sitelist, holelist, sample_depth, data_add], axis=1, ignore_index=False)
            for col in data:
                if 'section::inorganic carbon content:' in col:
                    data = data.rename(columns={col:'inorganic_carbon'})
                elif 'analysis::inorganic carbon content:' in col:
                    data = data.rename(columns={col:'inorganic_carbon'})
                elif 'section::CaCO3 content:' in col:
                    data = data.rename(columns={col:'calcium_carbonate'})
                elif 'analysis::CaCO3 content:' in col:
                    data = data.rename(columns={col:'calcium_carbonate'})
                elif 'analysis::total carbon' in col:
                    data = data.rename(columns={col:'total_carbon'})
                elif 'analysis::sulfur' in col:
                    data = data.rename(columns={col:'sulfur'})
                elif 'analysis::nitrogen' in col:
                    data = data.rename(columns={col:'nitrogen'})

            chikyu_data = pd.concat([chikyu_data,data], axis=0, sort=False)
            chikyu_data = chikyu_data[['leg','site','hole','sample_depth',
                           'inorganic_carbon','calcium_carbonate',
                           'total_carbon','sulfur','nitrogen']]
        else:
            continue

    chikyu_data = chikyu_data.reset_index(drop=True)
    chikyu_data = chikyu_data.applymap(str)
    return chikyu_data

def compile_cns():
    dsdp = load_dsdp_cns()
    odp = load_odp_cns()
    iodp = load_iodp_cns()
    chikyu = load_chikyu_cns()

    cns = pd.concat((dsdp, odp, iodp, chikyu), axis=0, sort=False).reset_index(drop=True)
    cns = cns.applymap(str)
    cns = cns.reset_index(drop=True)
    return cns


