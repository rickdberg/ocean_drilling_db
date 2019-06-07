#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 29 20:26:50 2019

@author: rick
Module with functions for importing and integrating interstitial water chemistry
data from DSDP, ODP, and IODP (Including Chikyu) into a single, standardized csv file.


Functions:

"""
import pandas as pd
import numpy as np
import os

from ocean_drilling_db import data_filepaths as dfp

def load_dsdp_iw():
    print('Loading DSDP IW...')
    dsdp_data = pd.read_csv(dfp.dsdp_iw, sep="\t", header=0,
                                skiprows=None, encoding='windows-1252')
    dsdp_data = dsdp_data.rename(columns={'depth to sample (m)':'sample_depth',
                                          'depth to core (m)':'core_depth',
                                          'bottom of sampled interval(cm)':'bottom',
                                          'top of sampled interval (cm)':'top'})

    dsdp_data = dsdp_data[dsdp_data['card type'] == 'DATA CARD']

    sample_keys = dsdp_data[['leg','site','hole','core','section',
                           'bottom','top','core_depth','sample_depth']].drop_duplicates()
    sample_keys = sample_keys.reset_index(drop=True).reset_index()
    sample_keys = sample_keys.rename(columns={'index':'sample_key'})
    dsdp_data = pd.merge(dsdp_data, sample_keys, how='left',
                         on=['leg','site','hole','core','section',
                           'bottom','top','core_depth','sample_depth'])

    anchor_df = dsdp_data[dsdp_data['card type'] == 'DATA CARD']
    anchor_df = dsdp_data[['sample_key','leg','site','hole','core','section',
                           'bottom','top','core_depth','sample_depth',
                           'pH electrode type','pH','alkalinity measurement type',
                           'alkalinity','salinity']]
    anchor_df = anchor_df.rename(columns={'pH electrode type':'ph_type',
                                          'alkalinity measurement type':'alkalinity_type'})
    anchor_df['rep_key'] = anchor_df.groupby('sample_key').cumcount()+1
    anchor_df = anchor_df.dropna(how='all',subset=['pH','alkalinity','salinity']).reset_index(drop=True)

    col_labels = [{'data field #1':'Ca','data field #2':'Mg',
               'data field #3':'Cl','data field #4':'NH4','data field #5':'PO4',
               'data field #6':'Si','reference':'ref_1'},
               {'data field #1':'Sr','data field #2':'K',
               'data field #3':'Mn','data field #4':'SO4','data field #5':'Ba',
               'data field #6':'Zn','reference':'ref_2'},
               {'data field #1':'P2O4','data field #2':'Cu',
               'data field #3':'Fe','data field #4':'Li','data field #5':'Al',
               'data field #6':'Na','reference':'ref_3'},
               {'data field #1':'Br','data field #2':'B',
               'data field #3':'Rb','data field #4':'Ni',
               'data field #6':'NO3','reference':'ref_4'},
               {'reference':'ref_5'},
               {'reference':'ref_6'}]

    cards = []
    for n in np.arange(6)+1:
        cards.append(dsdp_data[(dsdp_data['card number'] == n) & (dsdp_data['card type'] == 'DATA CARD')])

    for n, card in enumerate(cards):
        card = card[['sample_key','leg','site','hole','core','section', 'top',
                     'bottom','core_depth','sample_depth','data field #1',
                     'data field #2','data field #3','data field #4',
                     'data field #5','data field #6','reference']]
        card = card.rename(columns=col_labels[n])
        if n == 3:
            card = card.drop(columns=['data field #5'])
        elif n == 4 or n == 5:
            card = card.drop(columns=['data field #1','data field #2','data field #3',
                       'data field #4','data field #5','data field #6'])
        card['rep_key'] = card.groupby('sample_key').cumcount()+1
        cards[n] = card

    dsdp_std_final = anchor_df
    for card in cards:
        dsdp_std_final = pd.merge(dsdp_std_final, card, how='outer',
                              on=['sample_key', 'rep_key', 'leg', 'site', 'hole', 'core',
                                  'section', 'top', 'bottom', 'core_depth', 'sample_depth'])

    # Make specific substitutions and formatting
    dsdp_std_final = dsdp_std_final.replace(to_replace=['.'], value=np.nan)
    dsdp_std_final.fillna(value=np.nan, inplace=True)

    # Unit conversions
    dsdp_std_final['Cl'] = (dsdp_std_final['Cl'].astype(float)/35.453*1024).map(int, na_action='ignore')
    dsdp_std_final[['Sr','Zn','Cu','B']] = dsdp_std_final[['Sr','Zn','Cu','B']].astype(float)*1000
    dsdp_std_final['Li'] = dsdp_std_final['Li'].astype(float)/10

    dsdp_std_final = dsdp_std_final.applymap(str)
    print('DSDP IW loaded.')
    return dsdp_std_final


def load_odp_iw():
    print('Loading ODP IW...')
    odp_data = pd.read_csv(dfp.odp_iw, sep="\t", header=0,
                                    skiprows=None, encoding='windows-1252')
    odp_data = odp_data.applymap(str)

    odp_headers = ['leg', 'site', 'hole', 'core', 'type', 'section', 'top',
                   'bottom', 'sample_depth', 'Al', 'NH4', 'B', 'Br', 'Ca',
                   'Cl', 'F', 'I', 'Fe', 'Li', 'Mg', 'Mn', 'NO3', 'pH', 'PO4',
                   'K', 'Rb', 'Na', 'Sr', 'SO4', 'Si', 'alkalinity', 'salinity',
                   'Ba', 'Pb', 'H2', 'DIC', 'formate', 'ppH', 'DOC', 'acetate',
                   'NO2', 'color', 'sulfide', 'Zn']
    odp_data.columns = odp_headers
    for col in odp_data.columns:
        odp_data[col] = odp_data[col].str.strip() # remove leading and trailing whitespace

    # Create table of unique samples with sample keys
    odp_unique = odp_data[['leg', 'site', 'hole', 'core', 'type', 'section', 'top',
                           'bottom', 'sample_depth']]
    odp_unique = odp_unique.drop_duplicates().reset_index(drop=True).reset_index()
    odp_unique = odp_unique.rename(columns = {"index": "sample_key"}).applymap(str)

    # Join with data table to get sample_key applied to all rows
    odp_data = odp_data.merge(odp_unique, how='left', on=['leg', 'site', 'hole',
                                                           'core', 'type', 'section',
                                                           'top', 'bottom', 'sample_depth'])

    odp_data_std = pd.DataFrame(columns=['sample_key', 'rep_key'])
    for col in odp_data.columns.drop(list(odp_unique.columns)):
        all_data = odp_data[col].str.split('\s+').apply(pd.Series) # Split cells with multiple entries into separte rows
        all_data.index = odp_data['sample_key'] # Assign sample_keys to index
        all_data = all_data.stack().reset_index(0) # Stack columns into one column and pull out sample_keys along with each value
        all_data.columns = ['sample_key', odp_data[col].name]
        all_data['rep_key'] = all_data.groupby('sample_key').cumcount()+1
        odp_data_std = odp_data_std.merge(all_data, how='outer', on=['sample_key', 'rep_key'])
    odp_std_final = odp_data_std.merge(odp_unique, how='outer', on=['sample_key'])
    odp_std_final = odp_std_final.replace(to_replace='None', value=np.nan)
    odp_std_final.rep_key = odp_std_final.rep_key.map(int).map(str)

    odp_std_final = odp_std_final.replace(['','...'],np.nan)
    odp_std_final = odp_std_final.fillna(np.nan)

    # Make unit conversions to standard
    odp_std_final['NH4'] = odp_std_final['NH4'].astype(float)/1000
    odp_std_final['B'] = odp_std_final['B'].astype(float)*1000
    odp_std_final['Br'] = odp_std_final['Br'].astype(float)/1000
    odp_std_final['Pb'] = odp_std_final['Pb'].astype(float)*1000
    odp_std_final['NO2'] = odp_std_final['NO2'].astype(float)/1000
    odp_std_final['Zn'] = odp_std_final['Zn'].astype(float)*1000
    print('ODP IW loaded.')
    return odp_std_final


def load_iodp_iw():
    print('Loading IODP IW...')
    iodp_data = pd.read_csv(dfp.iodp_iw, sep=",", header=0,
                                        skiprows=None,
                                        encoding='windows-1252',
                                        low_memory=False)
    iodp_data = iodp_data.applymap(str)
    for x in iodp_data.columns:
        iodp_data[x] = iodp_data[x].str.strip() # remove leading and trailing whitespace
    iodp_data.shape

    # Load data from file
    # iodp_data_no_comm = iodp_data.iloc[:,:-5]

    # Make specific replacements
    iodp_data = iodp_data.replace(to_replace='320(321)', value='321')
    iodp_data = iodp_data.replace(to_replace=['nd', 'n.d.', 'ND', 'N.D.',
                                                              'bdl', 'BLD', 'bld', 'bd',
                                                              'BD', 'BDL', 'b.d.l.', 'B.D.L.'], value=0)
    iodp_data = iodp_data.replace(to_replace=['bld', 'bdl'], value=0, regex=True)
    iodp_data = iodp_data.replace(to_replace=['<\S+', '-\d+\.\d+', '-\d+'], value=0, regex=True)
    iodp_data = iodp_data.replace(to_replace=['-', 'invalid', 'Ã¢Â¿Â¿'], value=np.nan)
    iodp_data = iodp_data.applymap(str)
    #iodp_data = pd.concat([iodp_data, iodp_data_raw], axis=1)

    # Create table of unique samples
    id_cols = list(iodp_data.columns[:13])
    iodp_unique = iodp_data[id_cols]
    iodp_unique = iodp_unique.drop_duplicates()
    comment_cols = iodp_unique.reset_index()['index']
    iodp_unique = iodp_unique.reset_index(drop=True).reset_index()
    iodp_unique = iodp_unique.rename(columns = {"index": "sample_key"})
    iodp_unique = iodp_unique.applymap(str)

    # Join with data table to get sample_key applied to all rows
    iodp_data = iodp_data.merge(iodp_unique, how='outer', on=id_cols)
    iodp_data = iodp_data.applymap(str)

    # Add rep_key and split duplicates in single cells
    data_cols = list(iodp_data.columns[13:-6])
    iodp_data_std = pd.DataFrame(columns=['sample_key', 'rep_key'], dtype='str')
    for col_name in data_cols:
       df_all = iodp_data[col_name].str.split('[,](?!\s)').apply(pd.Series) # Split cells with multiple entries into separte rows
       df_all.index = iodp_data['sample_key'] # Assign sample_keys to index
       df_all = df_all.stack().reset_index(0) # Stack columns into one column and pull out sample_keys along with each value
       df_all.columns = ['sample_key', col_name]
       df_all['rep_key'] = df_all.groupby(['sample_key']).cumcount()+1
       df_all = df_all.reset_index(drop=True)
       iodp_data_std = iodp_data_std.merge(df_all, how='outer', on=['sample_key', 'rep_key'])
    iodp_analytes = iodp_data_std.applymap(float)

    # Make unit conversions to standard
    iodp_analytes['Al (uM) 309.3 nm ICPAES'] = iodp_analytes['Al (uM) 309.3 nm ICPAES'].astype(float)/1000
    iodp_analytes['Ammonium (ÂµM) SPEC'] = iodp_analytes['Ammonium (ÂµM) SPEC'].astype(float)/1000
    iodp_analytes['AMMONIUM (ÂµM) SPEC'] = iodp_analytes['AMMONIUM (ÂµM) SPEC'].astype(float)/1000
    iodp_analytes['ammonium (ÂµM) SPEC'] = iodp_analytes['ammonium (ÂµM) SPEC'].astype(float)/1000
    iodp_analytes['NITRATE_CD (mM) DA'] = iodp_analytes['NITRATE_CD (mM) DA'].astype(float)*1000
    iodp_analytes['NITRATE_LOW (mM) DA'] = iodp_analytes['NITRATE_LOW (mM) DA'].astype(float)*1000
    iodp_analytes['NITRITES_TEST (mM) DA'] = iodp_analytes['NITRITES_TEST (mM) DA'].astype(float)*1000
    iodp_analytes['PHOSPHATE (mM) DA'] = iodp_analytes['PHOSPHATE (mM) DA'].astype(float)*1000
    iodp_analytes['Phosphate (mM) DA'] = iodp_analytes['Phosphate (mM) DA'].astype(float)*1000
    iodp_analytes['SILICA (mM) DA'] = iodp_analytes['SILICA (mM) DA'].astype(float)*1000
    iodp_analytes['Silica (mM) DA'] = iodp_analytes['Silica (mM) DA'].astype(float)*1000
    iodp_analytes['SILICAPD (mM) DA'] = iodp_analytes['SILICAPD (mM) DA'].astype(float)*1000

    # Combine columns based on standardized names
    assign = pd.Series(('sample_key_temp', '0', 'rep_key_temp', '0', 'Al',
                        'alkalinity', 'NH4', 'NH4', 'NH4', 'NH4', 'NH4', '0',
                        'B', 'B', 'B', 'B', 'B', 'B', 'B', 'Ba', 'Ba', 'Ba', 'Ba',
                        'Ba', 'Ba', 'Ba', 'Ba', 'Ba', 'Ba', 'Br', 'Br', 'Ca', 'Ca',
                        'Ca', 'Ca', 'Ca', 'Ca', 'Ca', 'Ca', 'Ca_ic', 'Ca_ic',
                        'Cl_ic', 'Cl_ic', 'Cl', 'Cl_ic', 'Cs', 'DIC', 'Fe', 'Fe',
                        'Fe', 'Fe', 'Fe', 'Fe', '0', 'K', 'K', 'K', 'K', 'Li',
                        'Li', 'Li', 'Li', 'Li', 'Mg_ic', 'Mg_ic', 'Mg', 'Mg', 'Mg',
                        'Mg', 'Mg', 'Mg', 'Mn', 'Mn', 'Mn', 'Mn', 'Mn', 'Mn', 'Mo',
                        'Na_ic', 'Na', 'Na', 'Na', 'Na', 'Na', 'NO3', 'NO3_NO2',
                        'NO3', 'NO3', '0', '0', 'PO4', 'PO4', 'PO4', 'PO4', 'K_ic',
                        'K_ic', 'Rb', 'S', 'S', 'salinity', 'Si', 'Si', 'Si', 'Si',
                        'Si', 'Si', 'Si_spec', 'Si_spec', 'Si_spec', 'Si_spec',
                        '0', '0', 'Na_ic', 'Na_ic', 'Sr', 'Sr', 'Sr', 'Sr', 'Sr',
                        'Sr', 'SO4', 'SO4', 'sulfide', 'sulfide', '0', '0',
                        'U', 'V'))
    analytes = iodp_analytes.columns
    analyte_dict = dict(zip(analytes, assign))
    reduced_list = iodp_analytes.groupby(by=analyte_dict, axis=1).mean()
    reduced_list = reduced_list.iloc[:,1:]
    reduced_list = pd.concat([reduced_list, iodp_data_std.loc[:,['rep_key', 'sample_key']]], axis=1)
    reduced_list.rep_key = reduced_list.rep_key.map(int).map(str)

    # Calculate sample_depth
    sample_depth = pd.Series(name='sample_depth')
    for x in np.arange(len(iodp_unique)):
        if iodp_unique["Top depth CSF-A (m)"][x] == np.nan:
            sample_depth.at[x] = np.nan
        elif iodp_unique["Bottom depth CSF-A (m)"][x] == np.nan:
            sample_depth.at[x] = np.nan
        else:
            sample_d = np.average([iodp_unique["Top depth CSF-A (m)"].astype(float)[x], iodp_unique["Bottom depth CSF-A (m)"].astype(float)[x]])
            sample_depth.at[x] = sample_d

    # Standardize sample labels
    std_labels = ['leg', 'site', 'hole', 'core', 'type', 'section', 'aw', 'top', 'bottom']
    label_dict = dict(zip(iodp_data.columns[:9], std_labels))
    label_data = iodp_unique.iloc[:,:10].rename(columns=label_dict)
    label_data = pd.concat([label_data, sample_depth], axis = 1)

    # Join sample labels, analyte data, and comments
    iodp_final = label_data.merge(reduced_list, how='inner', on='sample_key')
    iodp_final = pd.merge(iodp_final, iodp_data.loc[comment_cols,['sample_key', 'Proceedings label', 'Comments']], how='outer', on='sample_key')

    # Create final iodp iw dataset
    iodp_std_final = iodp_final[['sample_key', 'rep_key', 'leg', 'site', 'hole',
                                 'core', 'type', 'section', 'aw', 'top', 'bottom',
                                 'sample_depth', 'Al', 'alkalinity', 'NH4', 'B',
                                 'Ba', 'Br', 'Ca', 'Ca_ic', 'Cl_ic', 'Cl', 'Cs',
                                 'DIC', 'Fe', 'K', 'Li', 'Mg_ic', 'Mg', 'Mn', 'Mo',
                                 'Na', 'Na_ic', 'NO3', 'NO3_NO2', 'K_ic', 'PO4',
                                 'Rb', 'S', 'salinity', 'Si', 'Si_spec', 'Sr',
                                 'SO4', 'sulfide', 'U', 'V', 'Proceedings label',
                                 'Comments']]
    iodp_std_final = iodp_std_final.rename(columns={'Proceedings label': 'proceedings_label'})
    print('IODP IW loaded.')
    return iodp_std_final


def load_chikyu_iw(hole_metadata):
    print('Loading Chikyu IW...')
    ##### File group info #####
    # use filenames that include 'bulk-pore-water-chemistry'

    counter = 0
    for filename in os.listdir(dfp.chikyu_iw):
        if filename.endswith(".csv"):
            file_path = os.path.join('data','chikyu','iw',filename)
            data_add = pd.read_csv(file_path, sep=",", header=0, skiprows=None)
            counter += 1
            # Find leg, site, hole
            leg = 'no_leg'
            # print(counter)
            for col in data_add.select_dtypes(include='object'):
                for n in np.arange(len(hole_metadata)):
                    hole_ids = (hole_metadata['site'].map(str) + hole_metadata['hole'])
                    if data_add[col].str.contains(hole_ids[n]).any() and 'C0' in hole_ids[n]:
                        leg = str(hole_metadata.loc[n,'leg'])
                        site = hole_metadata.loc[n,'site']
                        hole = hole_metadata.loc[n,'hole']
                        break

                if leg != 'no_leg':
                    break
            #Add in leg, site, hole, sample_depth
            leglist = [leg] * len(data_add)
            leglist = pd.Series(leglist, name='leg', dtype='str')
            sitelist = [site] * len(data_add)
            sitelist = pd.Series(sitelist, name='site', dtype='str')
            holelist = [hole] * len(data_add)
            holelist = pd.Series(holelist, name='hole', dtype='str')
            sample_depth = (data_add['Top Depth DSF, MSF, WSF and CSF-A [m]'] +
                            data_add['Bottom Depth DSF, MSF, WSF and CSF-A [m]'])/2
            sample_depth.name = 'sample_depth'
            data_add = pd.concat([leglist, sitelist, holelist, sample_depth, data_add], axis=1, ignore_index=False)
            data_add = data_add.applymap(str)
            if counter == 1:
                chikyu_data = data_add.copy()
                chikyu_data = chikyu_data.applymap(str)
            else:
                chikyu_data = chikyu_data.merge(data_add, how='outer')
        else:
            continue
        # print(counter)
    ###############################################################################
    for x in chikyu_data.columns:
        chikyu_data[x] = chikyu_data[x].str.strip()  # remove leading and trailing whitespace

    # Create table of unique samples to get sample_keys
    chikyu_unique = chikyu_data[['leg','site','hole','sample_depth']]
    chikyu_unique = chikyu_unique.drop_duplicates()
    chikyu_unique = chikyu_unique.reset_index(drop=True)
    chikyu_unique = chikyu_unique.reset_index()
    chikyu_unique = chikyu_unique.rename(columns = {"index": "sample_key"})
    chikyu_unique = chikyu_unique.applymap(str)

    # Join with data table to get sample_key applied to all rows
    chikyu_data = chikyu_data.merge(chikyu_unique,
                                    how='outer',
                                    on=['leg','site','hole','sample_depth'])
    chikyu_data = chikyu_data.applymap(str)

    chikyu_data['rep_key'] = chikyu_data.groupby(['sample_key']).cumcount()+1
    chikyu_data['rep_key'] = chikyu_data['rep_key'].map(int).map(str)

    # Make unit conversions to standard
    chikyu_data['pore water chemistry; sample::Si concentration: UV-Visible spectrophotometer [mM]::number'] = chikyu_data['pore water chemistry; sample::Si concentration: UV-Visible spectrophotometer [mM]::number'].astype(float)*1000
    chikyu_data['pore water chemistry; sample::Rb concentration: ICP-MS [nM]::number'] = chikyu_data['pore water chemistry; sample::Rb concentration: ICP-MS [nM]::number'].astype(float)/1000
    chikyu_data['pore water chemistry::NO3 concentration: IC [mM]::number'] = chikyu_data['pore water chemistry::NO3 concentration: IC [mM]::number'].astype(float)*1000
    chikyu_data['pore water chemistry::Rb concentration: ICP-MS [nM]::number'] = chikyu_data['pore water chemistry::Rb concentration: ICP-MS [nM]::number'].astype(float)/1000
    chikyu_data['pore water chemistry; sample::NH4 concentration: UV-Visible spectrophotometer [µM]::number'] = chikyu_data['pore water chemistry; sample::NH4 concentration: UV-Visible spectrophotometer [µM]::number'].astype(float)/1000
    chikyu_data['pore water chemistry; sample::Br concentration: IC [µM]::number'] = chikyu_data['pore water chemistry; sample::Br concentration: IC [µM]::number'].astype(float)/1000
    chikyu_data['pore water chemistry; sample::NO3 concentration: IC [mM]::number'] = chikyu_data['pore water chemistry; sample::NO3 concentration: IC [mM]::number'].astype(float)*1000

    # Combine columns based on standardized names
    chikyu_analytes = chikyu_data[['pore water chemistry; sample::refractive index nD: refractometer::number',
                                   'pore water chemistry; sample::chlorinity: titrator, potentiometric titration [mM]::number',
                                   'pore water chemistry; sample::Li concentration: ICP-AES [µM]::number',
                                   'pore water chemistry; sample::B concentration: ICP-AES [µM]::number',
                                   'pore water chemistry; sample::NH4 concentration: UV-Visible spectrophotometer [mM]::number',
                                   'pore water chemistry; sample::Na concentration: IC [mM]::number',
                                   'pore water chemistry; sample::Mg concentration: IC [mM]::number',
                                   'pore water chemistry; sample::Si concentration: ICP-AES [µM]::number',
                                   'pore water chemistry; sample::Si concentration: UV-Visible spectrophotometer [mM]::number',
                                   'pore water chemistry; sample::PO4 concentration: UV-Visible spectrophotometer [µM]::number',
                                   'pore water chemistry; sample::SO4 concentration: IC [mM]::number',
                                   'pore water chemistry; sample::K concentration: IC [mM]::number',
                                   'pore water chemistry; sample::Ca concentration: IC [mM]::number',
                                   'pore water chemistry; sample::Mn concentration: ICP-AES [µM]::number',
                                   'pore water chemistry; sample::Fe concentration: ICP-AES [µM]::number',
                                   'pore water chemistry; sample::Zn concentration: ICP-MS [nM]::number',
                                   'pore water chemistry; sample::Br concentration: IC [mM]::number',
                                   'pore water chemistry; sample::Rb concentration: ICP-MS [nM]::number',
                                   'pore water chemistry; sample::Sr concentration: ICP-AES [µM]::number',
                                   'pore water chemistry; sample::Mo concentration: ICP-MS [nM]::number',
                                   'pore water chemistry; sample::Cs concentration: ICP-MS [nM]::number',
                                   'pore water chemistry; sample::Ba concentration: ICP-AES [µM]::number',
                                   'pore water chemistry; sample::U concentration: ICP-MS [nM]::number',
                                   'pore water chemistry::pmH: pH electrode, attached to titrator::number',
                                   'pore water chemistry::alkalinity: titrator [mM]::number',
                                   'pore water chemistry; sample::V concentration: ICP-MS [nM]::number',
                                   'pore water chemistry; sample::Cu concentration: ICP-MS [nM]::number',
                                   'pore water chemistry; sample::Pb concentration: ICP-MS [nM]::number',
                                   'pore water chemistry::refractive index nD: refractometer::number',
                                   'pore water chemistry::salinity: refractometer [permil]::number',
                                   'pore water chemistry::PO4 concentration: UV-Visible spectrophotometer [µM]::number',
                                   'pore water chemistry::NH4 concentration: UV-Visible spectrophotometer [mM]::number',
                                   'pore water chemistry::Cl concentration: IC [mM]::number',
                                   'pore water chemistry::Br concentration: IC [mM]::number',
                                   'pore water chemistry::NO3 concentration: IC [mM]::number',
                                   'pore water chemistry::SO4 concentration: IC [mM]::number',
                                   'pore water chemistry::Na concentration: IC [mM]::number',
                                   'pore water chemistry::K concentration: IC [mM]::number',
                                   'pore water chemistry::Mg concentration: IC [mM]::number',
                                   'pore water chemistry::Ca concentration: IC [mM]::number',
                                   'pore water chemistry::B concentration: ICP-AES [µM]::number',
                                   'pore water chemistry::Ba concentration: ICP-AES [µM]::number',
                                   'pore water chemistry::Fe concentration: ICP-AES [µM]::number',
                                   'pore water chemistry::Li concentration: ICP-AES [µM]::number',
                                   'pore water chemistry::Mn concentration: ICP-AES [µM]::number',
                                   'pore water chemistry::Si concentration: ICP-AES [µM]::number',
                                   'pore water chemistry::Sr concentration: ICP-AES [µM]::number',
                                   'pore water chemistry::chlorinity: titrator, potentiometric titration [mM]::number',
                                   'pore water chemistry::V concentration: ICP-MS [nM]::number',
                                   'pore water chemistry::Cu concentration: ICP-MS [nM]::number',
                                   'pore water chemistry::Zn concentration: ICP-MS [nM]::number',
                                   'pore water chemistry::Rb concentration: ICP-MS [nM]::number',
                                   'pore water chemistry::Mo concentration: ICP-MS [nM]::number',
                                   'pore water chemistry::Cs concentration: ICP-MS [nM]::number',
                                   'pore water chemistry::Pb concentration: ICP-MS [nM]::number',
                                   'pore water chemistry::U concentration: ICP-MS [nM]::number',
                                   'pore water chemistry; sample::NH4 concentration: UV-Visible spectrophotometer [µM]::number',
                                   'pore water chemistry; sample::Na concentration: ICP-AES [mM]::number',
                                   'pore water chemistry; sample::Mg concentration: ICP-AES [mM]::number',
                                   'pore water chemistry; sample::K concentration: ICP-AES [mM]::number',
                                   'pore water chemistry; sample::Ca concentration: ICP-AES [mM]::number',
                                   'pore water chemistry; sample::Br concentration: IC [µM]::number',
                                   'pore water chemistry; sample::salinity: refractometer [permil]::number',
                                   'pore water chemistry; sample::Na concentration: charge balance [mM]::number',
                                   'pore water chemistry; sample::SO4 concentration: selected from IC without or with Cd(NO3)2 [mM]::number',
                                   'pore water chemistry; sample::SO4 concentration: IC with Cd(NO3)2 [mM]::number',
                                   'pore water chemistry; sample::Cl concentration: chlorinity - Br [mM]::number',
                                   'pore water chemistry; sample::Rb concentration: ICP-MS [µM]::number',
                                   'pore water chemistry; sample::NO2 concentration: IC [mM]::number',
                                   'pore water chemistry; sample::NO3 concentration: IC [mM]::number',
                                   'pore water chemistry; sample::HS concentration: spectrophotometer, 3rd party [µM]::number',
                                   'pore water chemistry; sample::Fe(II) concentration: spectrophotometer, 3rd party [µM]::number',
                                   'pore water chemistry; sample::DIC concentration: coulometer, DIC-EXIT, 3rd party [mM]::number']].applymap(float)
    analytes_std = pd.Series(('refractive_index', 'Cl', 'Li', 'B', 'NH4', 'Na_ic',
                              'Mg_ic', 'Si', 'Si_spec', 'PO4', 'SO4', 'K_ic',
                              'Ca_ic', 'Mn', 'Fe', 'Zn', 'Br', 'Rb', 'Sr', 'Mo',
                              'Cs', 'Ba', 'U', 'pH', 'alkalinity', 'V', 'Cu', 'Pb',
                              'refractive_index', 'salinity', 'PO4', 'NH4', 'Cl_ic',
                              'Br', 'NO3', 'SO4', 'Na_ic', 'K_ic', 'Mg_ic', 'Ca_ic',
                              'B', 'Ba', 'Fe', 'Li', 'Mn', 'Si', 'Sr', 'Cl', 'V',
                              'Cu', 'Zn', 'Rb', 'Mo', 'Cs', 'Pb', 'U', 'NH4', 'Na',
                              'Mg', 'K', 'Ca', 'Br', 'salinity', 'Na', 'SO4', 'SO4',
                              'Cl', 'Rb', 'NO2', 'NO3', 'sulfide', 'Fe_spec', 'DIC'))
    analytes_orig = chikyu_analytes.columns
    analyte_dict = dict(zip(analytes_orig, analytes_std))
    analytes_reduced = chikyu_analytes.groupby(by=analyte_dict, axis=1).mean()

    chikyu_labels = chikyu_data[['sample_key', 'rep_key', 'leg', 'site', 'hole',
                                 'sample_depth', 'Sample comment',
                                 'pore water chemistry::comment on measurement::text']]
    labels_std = pd.Series(('sample_key', 'rep_key', 'leg', 'site', 'hole',
                            'sample_depth', 'Comments', 'More _comments'))
    labels_orig = chikyu_labels.columns
    labels_dict = dict(zip(labels_orig, labels_std))
    chikyu_idents = chikyu_labels.rename(columns=labels_dict)

    # Join sample labels, analyte data, and comments
    chikyu_std_final = pd.concat([chikyu_idents, analytes_reduced], axis=1)

    print('Chikyu IW loaded.')
    return chikyu_std_final


def compile_iw(site_metadata):
    dsdp_iw = load_dsdp_iw()
    odp_iw = load_odp_iw()
    iodp_iw = load_iodp_iw()
    chikyu_iw = load_chikyu_iw(site_metadata)

    iw = pd.concat((dsdp_iw, odp_iw, iodp_iw, chikyu_iw), axis=0, sort=False).reset_index(drop=True)
    iw = iw[(~iw['leg'].str.contains('QAQC')) & (~iw['leg'].str.contains('TEST'))]
    iw = iw.applymap(str)
    iw = iw.reset_index(drop=True)
    return iw


















