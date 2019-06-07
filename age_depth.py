#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 13 14:18:46 2018

@author: rick

Module with functions for importing and integrating biostratigraphic
age-depth data from DSDP, ODP, and IODP into a single, standardized csv file.

Age-depth data are not available for Chikyu expeditions.

IODP: Must first manually download each "List of Assets" from IODP LIMS DESC Reports web portal with "workbook" and "fossil" selected

Functions:

"""
import glob
import os
import pandas as pd
import numpy as np

from ocean_drilling_db import data_filepaths as dfp

def load_dsdp_age_depth():
    # Read in data and rename columns
    dsdp_data = pd.read_csv(dfp.dsdp_age_depth, sep="\t", header=0,
                            skiprows=None, encoding='windows-1252')
    dsdp_data = dsdp_data.reindex(['leg', 'site', 'hole', 'top of section depth(m)',
                                   'bottom of section depth(m)', 'age top of section(million years)',
                                   'age bottom of section(million years)', 'data source'], axis=1)
    dsdp_data.columns = ('leg', 'site', 'hole', 'top_depth', 'bottom_depth',
                         'top_age', 'bottom_age', 'source')

    dsdp_data[['top_age', 'bottom_age']] = np.multiply(dsdp_data[['top_age', 'bottom_age']], 1000000)
    dsdp_data = dsdp_data.applymap(str)

    # Assign site keys
    site_keys = pd.read_csv('hole_metadata.csv', sep='\t', index_col=0)
    site_keys = site_keys[['site_key','site']]

    full_data = pd.merge(site_keys, dsdp_data, how = 'inner', on = 'site')
    full_data = full_data.reindex(['site_key', 'leg', 'site', 'hole',
                                   'top_depth', 'bottom_depth', 'top_age',
                                   'bottom_age', 'type', 'source'], axis=1)

    # Use both top and bottom picks
    top_values = full_data.reindex(['site_key', 'leg', 'site', 'hole',
                                    'top_depth','top_age', 'type', 'source'], axis=1)
    top_values = top_values.rename(columns={'top_depth': 'depth', 'top_age': 'age'})
    bottom_values = full_data.reindex(['site_key', 'leg', 'site', 'hole',
                                       'bottom_depth', 'bottom_age', 'type', 'source'], axis=1)
    bottom_values = bottom_values.rename(columns={'bottom_depth': 'depth', 'bottom_age': 'age'})
    final_data = pd.concat([top_values, bottom_values])
    final_data[['age', 'depth']] = final_data.loc[:,['age', 'depth']].applymap(float)

    # Sort and clean
    final_data = final_data.sort_values(['site_key', 'depth'])
    final_data = final_data.replace(to_replace='nan', value=np.nan)

    return final_data


### Difference between age-depth and age-profiles files??
def load_odp_age_depth():
    odp_data = pd.read_csv(dfp.odp_age_depth, sep="\t", header=0,
                           skiprows=None, encoding='windows-1252')

    # Rename and reorder columns, change units to years
    odp_data.columns = ('leg', 'site', 'hole', 'source', 'depth', 'age', 'type')
    odp_data = odp_data.reindex(['leg', 'site', 'hole', 'depth', 'age', 'type', 'source'], axis=1)
    odp_data['age'] = np.multiply(odp_data['age'], 1000000)
    odp_data = odp_data.applymap(str)

    # Assign site keys
    site_keys = pd.read_csv('hole_metadata.csv', sep='\t', index_col=0)
    site_keys = site_keys[['site_key','site']]
    full_data = pd.merge(site_keys, odp_data, how = 'inner', on = 'site')
    full_data = full_data.reindex(['site_key', 'leg', 'site', 'hole', 'depth', 'age', 'type', 'source'], axis=1)
    full_data[['age', 'depth']] = full_data.loc[:,['age', 'depth']].applymap(float)

    return full_data

def load_odp_age_profiles():
    data = pd.read_csv(dfp.odp_age_profile, sep="\t", header=0,
                           skiprows=None, encoding='windows-1252')
    # Filter out those with depth difference greater than 1 core length (10m) (11m to account for 10% error/expansion)
    diff = data['Ageprofile Depth Base']-data['Ageprofile Depth Top']
    data = data.iloc[diff[diff < 11].index.tolist(),:]
    data['Ageprofile Age Old'] = data['Ageprofile Age Old'].str.strip().replace('',np.nan).astype(float)

    # Average depths and ages
    data['depth'] = (data['Ageprofile Depth Top'] + data['Ageprofile Depth Base'])/2
    data['age'] = (data['Ageprofile Depth Top'] + data['Ageprofile Depth Base'])/2
    data.columns = data.columns.str.strip()

    data = data.reindex(['Leg', 'Site', 'Hole', 'depth', 'age',
                         'Ageprofile Datum Description'], axis=1)
    data = data.rename(columns={'Leg':'leg', 'Site':'site', 'Hole':'hole',
                         'Ageprofile Datum Description': 'type'})
    data.hole = data.hole.str.strip()
    data.type = data.type.str.strip()
    data.site = data['site'].astype(str)

    # Get site keys and add to DataFrame
    site_keys = pd.read_csv('hole_metadata.csv', sep='\t', index_col=0)
    site_keys = site_keys[['site_key','site']]

    full_data = pd.merge(site_keys, data, how='inner', on='site')
    full_data = full_data[['site_key', 'leg', 'site', 'hole', 'depth', 'age', 'type']]
    full_data['age'] = full_data['age'] * 1000000

    return full_data

def load_iodp_age_depth():

    files = glob.glob(os.path.join(dfp.iodp_age_depth,'*.xls*'))

    fossil_data = pd.DataFrame()
    for file in files:
        sheets = pd.ExcelFile(file).sheet_names
        if 'Age Control' in sheets:
            sheet_data = pd.read_excel(file, 'Age Control')
            fossil_data = fossil_data.append(sheet_data, sort=True)
    fossil_data = fossil_data.reset_index()

    # Cut to relevant data
    no_data  = np.where(fossil_data['Sample'].str.contains('No data.*', ) == True)[0]
    fossil_data = fossil_data.drop(no_data, axis=0)
    fossil_data = fossil_data.dropna(axis=1, how='all')
    column_list = list(fossil_data.columns)

    cols_orig = pd.Series(column_list)
    cols_std = pd.Series(('index', 'aw', 'depth_bottom', 'depth_bottom', 'bottom', 'depth_bottom', 'bottom', 'core',
                          'core_sect', 'datum', 'datum', 'age', 'age_old', 'age_young', 'age', 'age_old', 'age_young',
                          'age_old', 'age_young', 'datum_author_year', 'datum_author_year', 'comment', 'datum_group',
                          'datum_group_code', 'datum', 'datum_generic', 'datum_generic', 'datum_region', 'datum_status',
                          'datum_type', 'datum_val_comment', 'leg', 'sample_id_data', 'file_data', 'datum_generic', 'hole',
                          'label', 'marker_species', 'piece', 'label', 'section', 'file_links', 'site', 'depth_top',
                          'depth_top', 'top', 'depth_top', 'top', 'type', 'misc1', 'misc2', 'age_old', 'age_young',
                          'datum_author_year', 'datum_group', 'datum_group_code', 'datum', 'datum_generic', 'datum_status',
                          'datum_type', 'datum_val_comment', 'comments_other'))
    cols_dict = dict(zip(cols_orig, cols_std))
    fossil_data_reduced = fossil_data.groupby(by=cols_dict, axis=1).first()
    fossil_data_reduced = fossil_data_reduced.reindex(['label', 'leg', 'site', 'hole', 'depth_bottom',
                                                       'depth_top', 'age', 'age_old', 'age_young'], axis=1)

    # Remove those w no age data
    fossil_data_final = fossil_data_reduced.dropna(subset=['age', 'age_old', 'age_young'], how='all')
    fossil_data_final = fossil_data_final.reset_index()
    fossil_data_final.loc[pd.isnull(fossil_data_final['age_young']), 'age_old'] = np.nan
    fossil_data_final.loc[fossil_data_final['age_old'].isna(), 'age_young'] = np.nan


    # Filter out non-numeric notes in age records
    fossil_data_final['age'] = abs(fossil_data_final['age'])
    fossil_data_final[['age', 'age_old', 'age_young']] = fossil_data_final[['age', 'age_old', 'age_young']].applymap(str)
    fossil_data_final['age'] = fossil_data_final['age'].str.strip()
    fossil_data_final['age_old'] = fossil_data_final['age_old'].str.strip(to_strip = 'Ma')
    fossil_data_final['age_young'] = fossil_data_final['age_young'].str.strip()

    # Average ranges
    for m in ['age','age_old','age_young']:
        for n in range(len(fossil_data_final)):
            if '-' in fossil_data_final.loc[n,m]:
                age_range = fossil_data_final.loc[n,m].split('-')
                if '' in age_range:
                    fossil_data_final.loc[n,m] = age_range[1]
                else:
                    age_range = [float(i) for i in age_range]
                    fossil_data_final.loc[n,m] = np.mean(age_range)

    # Average depths
    fossil_data_final['depth'] = pd.DataFrame([fossil_data_final['depth_bottom'], fossil_data_final['depth_top']]).mean()
    fossil_data_final[['leg','label']] = fossil_data_final[['leg','label']].astype(str)
    fossil_data_final['leg'] = fossil_data_final['leg'].str.replace('\.0','')

    # Add leg, site, hole to all records
    for n in range(len(fossil_data_final)):
        if fossil_data_final.loc[n,'label'] != 'nan':
            fossil_data_final.loc[n,'leg'] = fossil_data_final.loc[n,'label'].split('-')[0]
            fossil_data_final.loc[n,'site'] = fossil_data_final.loc[n,'label'].split('-')[1][:5]
            fossil_data_final.loc[n,'hole'] = fossil_data_final.loc[n,'label'].split('-')[1][5]

    # Filter out those with depth difference greater than 1 core length (10m) (11m to account for 10% error/expansion)
    fdf = fossil_data_final
    diff = fdf['depth_bottom']-fdf['depth_top']
    fdf = fdf.iloc[diff[diff < 11].index.tolist(),:]

    # Assign site keys
    site_keys = pd.read_csv('hole_metadata.csv', sep='\t', index_col=0)
    site_keys = site_keys[['site_key','site']]
    fdf = pd.merge(site_keys, fdf, how = 'inner', on = 'site')
    fdf = fdf.reindex(['site_key', 'leg', 'site', 'hole', 'depth', 'age', 'age_old', 'age_young'], axis=1)

    # Assign ages
    fdf[['age_old','age_young','age']] = fdf[['age_old','age_young','age']].astype(float)
    fdf = fdf.replace('nan', np.nan)
    for n in fdf.index:
        if np.isnan(fdf.loc[n,'age']):
            if np.isnan(fdf.loc[n,'age_young']):
                fdf.loc[n,'age'] = fdf.loc[n,'age_old']
            elif np.isnan(fdf.loc[n,'age_old']):
                fdf.loc[n,'age'] = fdf.loc[n,'age_young']
            else:
                fdf.loc[n,'age'] = (fdf.loc[n,'age_old'] + fdf.loc[n,'age_young'])/2

    fdf['age'] = fdf['age'] * 1000000
    fdf = fdf.reindex(['site_key', 'leg', 'site', 'hole', 'depth', 'age'], axis=1)

    return fdf


def compile_age_depth():
    dsdp = load_dsdp_age_depth()
    odp = load_odp_age_depth()
    odp_p = load_odp_age_profiles()
    iodp = load_iodp_age_depth()

    age_depth = pd.concat((dsdp, odp, odp_p, iodp), axis=0, sort=False).reset_index(drop=True)
    for col in age_depth:
        if age_depth[col].dtype == object:
            age_depth[col] = age_depth[col].str.strip()
    age_depth = age_depth.replace('', np.nan)
    age_depth = age_depth.applymap(str)
    age_depth = age_depth.reset_index(drop=True)
    return age_depth


