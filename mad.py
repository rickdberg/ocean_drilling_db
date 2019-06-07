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

def load_dsdp_mad():
    # Read in data and rename columns
    dsdp_data = pd.read_csv(dfp.dsdp_mad, sep="\t", header=0,
                            skiprows=None, encoding='windows-1252')
    dsdp_data = dsdp_data.rename(columns={'sample depth (m)':'sample_depth',
                              'grain density (g/cc)':'grain_density'})
    dsdp_data = dsdp_data[['leg','site','hole','core','section','sample_depth',
                           'porosity','grain_density']]
    for col in dsdp_data:
        if dsdp_data[col].dtype == object:
            dsdp_data[col] = dsdp_data[col].str.strip()
    dsdp_data = dsdp_data.replace('', np.nan)
    dsdp_data['porosity'] = dsdp_data['porosity']/100
    return dsdp_data


def load_odp_mad():
    # Read in data and rename columns
    odp_data = pd.read_csv(dfp.odp_mad, sep="\t", header=0,
                            skiprows=None, encoding='windows-1252', low_memory=False)
    odp_data = odp_data.rename(columns={'Leg':'leg', 'Site':'site', 'H':'hole',
                                        'Cor':'core','Sc':'section',
                                        'Depth (mbsf)':'sample_depth',
                                        'GD (g/cc)':'grain_density',
                                        'PO (%)':'porosity','Method':'method'})
    odp_data = odp_data[['leg','site','hole','core','section','sample_depth',
                           'porosity','grain_density','method']]
    for col in odp_data:
        if odp_data[col].dtype == object:
            odp_data[col] = odp_data[col].str.strip()
    odp_data = odp_data.replace('', np.nan)
    odp_data['porosity'] = odp_data['porosity'].astype(float)/100
    return odp_data

def load_iodp_mad():
    # Read in data and rename columns
    iodp_data = pd.read_csv(dfp.iodp_mad, sep=",", header=0,
                            skiprows=None, encoding='windows-1252')
    iodp_data = iodp_data.replace(to_replace='320(321)', value='321')
    iodp_data = iodp_data.rename(columns={'Exp':'leg', 'Site':'site',
                                          'Hole':'hole', 'Core':'core',
                                          'Sect':'section','Depth CSF-A (m)':'sample_depth',
                                          'Submethod':'method', 'Grain density (g/cmÂ³)':'grain_density',
                                          'Porosity (vol%)':'porosity'})
    iodp_data = iodp_data[['leg','site','hole','core','section','sample_depth',
                           'porosity','grain_density','method']]
    for col in iodp_data:
        if iodp_data[col].dtype == object:
            iodp_data[col] = iodp_data[col].str.strip()
    iodp_data = iodp_data.replace('', np.nan)
    iodp_data['porosity'] = iodp_data['porosity']/100
    iodp_data = iodp_data[~iodp_data['leg'].isin(['QAQC','TEST'])]
    iodp_data['leg'] = iodp_data['leg'].replace('345(147)', '345')
    iodp_data['leg'] = iodp_data['leg'].replace('327(301)', '327')
    iodp_data['leg'] = iodp_data['leg'].replace('335(312)', '335')
    return iodp_data

def load_chikyu_mad():
    chikyu_data = pd.DataFrame()
    ##### File group info #####
    summary = pd.read_csv(dfp.chikyu_meta, sep=",", header=0, skiprows=None)
    summary = summary.iloc[1:,:].reset_index(drop=True)
    summary['leg'] = summary['EXPNAME']
    for n, hole_id in enumerate(summary.HOLENAME):
        summary.loc[n,'hole'] = hole_id[-1]
        summary.loc[n,'site'] = hole_id[:-1]

    for filename in os.listdir(dfp.chikyu_mad):
        if filename.endswith(".csv"):
            file_path = os.path.join('data','chikyu','mad',filename)
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
                if 'grain density' in col:
                    data = data.rename(columns={col:'grain_density'})
                elif 'porosity' in col:
                    data = data.rename(columns={col:'porosity'})
            data = data[['leg','site','hole','sample_depth',
                           'porosity','grain_density']]
            chikyu_data = pd.concat([chikyu_data,data], axis=0)
        else:
            continue
    chikyu_data = chikyu_data.reset_index(drop=True)
    return chikyu_data

def compile_mad():
    dsdp = load_dsdp_mad()
    odp = load_odp_mad()
    iodp = load_iodp_mad()
    chikyu = load_chikyu_mad()

    mad = pd.concat((dsdp, odp, iodp, chikyu), axis=0, sort=False).reset_index(drop=True)
    mad = mad.applymap(str)
    mad = mad.reset_index(drop=True)
    return mad


