# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import datetime as dt
import os
import io
import requests
from dateutil.relativedelta import relativedelta
from Modules.Utils import Listador, FindOutlier, FindOutlierMAD, Cycles
from Modules.Graphs import GraphSerieOutliers, GraphSerieOutliersMAD

def SSTregions():
    """
    Read SST weekly anomalies and put them in a DataFrame
    OUPUTS:
    SST : DataFrame with the anomalies of SST in El Niño regions
    """

    SSTweek = 'https://www.cpc.ncep.noaa.gov/data/indices/wksst8110.for'

    s  = requests.get(SSTweek).content

    date  = []
    N12   = []
    N12_A = []
    N3    = []
    N3_A  = []
    N34   = []
    N34_A = []
    N4    = []
    N4_A  = []

    with io.StringIO(s.decode('utf-8')) as f:
        data = f.readlines()
    for d in data[4:]:
        d = d.strip()
        d = d.split('     ')
        date.append(dt.datetime.strptime(d[0], '%d%b%Y'))
        N12  .append(float(d[1][:4]))
        N12_A.append(float(d[1][4:]))
        N3   .append(float(d[2][:4]))
        N3_A .append(float(d[2][4:]))
        N34  .append(float(d[3][:4]))
        N34_A.append(float(d[3][4:]))
        N4   .append(float(d[4][:4]))
        N4_A .append(float(d[4][4:]))

    SST = pd.DataFrame(np.array([N12_A,N3_A,N34_A,N4_A]).T, index=date, \
                       columns=[u'Niño1+2',u'Niño3',u'Niño34',u'Niño4'])
    return SST

def ONIdata():
    """
    Read ONI data and put them in a DataFrame
    OUPUTS:
    ONI : DataFrame with the ONI data
    """
    linkONI = 'https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt'
    s = requests.get(linkONI).content

    Season = []
    year   = []
    Total  = []
    Anom   = []
    date   = []

    with io.StringIO(s.decode('utf-8')) as f:
        data = f.readlines()
    m = 0
    for d in data[1:]:
        d = d.strip()
        d = d.split()
        Season.append(d[0])
        year  .append(int(d[1]))
        Total .append(float(d[2]))
        Anom  .append(float(d[3]))
        date  .append(dt.datetime(1950,2,1)+relativedelta(months=m))
        m+=1
    ONI = pd.DataFrame(np.array([Anom, Total, Season]).T, index=date, \
                       columns=[u'Anomalie', u'Total',u'Season'])

    return ONI

def SOIdata():
    """
    Read ONI data and put them in a DataFrame
    OUPUTS:
    ONI : DataFrame with the ONI data
    """
    # linkSOI = 'http://www.bom.gov.au/climate/enso/soi.txt'
    # linkSOI = 'https://www.ncdc.noaa.gov/teleconnections/enso/indicators/soi/data.csv'
    linkSOI = 'https://www.cpc.ncep.noaa.gov/data/indices/soi'
    s = requests.get(linkSOI).content

    date = []
    soi  = []

    with io.StringIO(s.decode('utf-8')) as f:
        data = f.readlines()

    # Old version with  csv data
    # m = 0
    # for i in range(len(data)):
    #     if i >=2:
    #         row = data[i].strip()
    #         val = row.split(',')
    #         date.append(dt.datetime.strptime(val[0], '%Y%m'))
    #         soi.append(float(val[1]))
    # SOI = pd.DataFrame(np.array(soi).T, index=date, columns=[u'SOI'])
    
    standarrized_flag = False
    for i in range(len(data)):
        if 'STANDARDIZED' in data[i]:
            standarrized_flag  = True

        if standarrized_flag == False:
            continue
        row = data[i].strip()
        if len(row) != 76:
            continue
        if row.startswith('Y') == True:
            continue

        year = int(row[:4])
        for m in range(12):
            a = float(row[6*(m)+4:6*(m+1)+4])

            date.append(dt.datetime(year, m+1,1))
            if a == -999.9:
                a = np.nan
            soi.append(a)

    SOI = pd.DataFrame(np.array(soi).T, index=date, columns=[u'SOI'])
    SOI = SOI.dropna()

    return SOI

def MEIdata():
    """
    Read ONI data and put them in a DataFrame
    OUPUTS:
    ONI : DataFrame with the ONI data
    """
    linkMEI = 'https://psl.noaa.gov/enso/mei/data/meiv2.data'
    s = requests.get(linkMEI).content

    date = []
    mei  = []

    with io.StringIO(s.decode('utf-8')) as f:
        data = f.readlines()
    lims = np.array(data[0].strip().split('   ')).astype(int)
    for i in range(len(data)):
        if i >=1:
            row = data[i].strip()
            val = row.split('    ')
            for m in range(12):
                date.append(dt.datetime(int(val[0]),m+1,1))
            mei.append(np.array(val[1:]).astype(float))
            if int(val[0])== lims[1]-1:
                break
    mei = np.array(mei).reshape(len(mei)*12)
    MEI = pd.DataFrame(np.array(mei).astype(float), index=date, columns=[u'MEI'])

    return MEI


def OuliersENSOjust(Serie, ENSO, method='IQR', lim_inf=0,
                    write=True, name=None,
                    graph=True, label='', title='', pdf=False, png=True, Path_Out=''):
    """
    Remove  outliers with the function find outliers and justify the values in ENSO periods
    INPUTS
    Serie : Pandas DataFrame or pandas Series with index as datetime
    ENSO  : Pandas DataFrame with the index of dates of ENSO periods
    method: str to indicate the mehtod to find outliers, ['IQR','MAD']
    lim_inf : limit at the bottom for the outliers
    write : boolean to write the outliers
    Name  : string of estation name to save the outliers
    label : string of the label
    title : Figure title
    pdf   : Boolean to save figure in pdf format
    png   : Boolean to save figure in png format
    Path_Out: Directoty to save figures
    OUTPUTS
    S : DataFrame without outliers outside ENSO periods
    """
    if method == 'IQR':
        idx = FindOutlier(Serie, clean=False, index=True, lims=False, restrict_inf=lim_inf)
    elif method == 'MAD':
        idx = FindOutlierMAD(Serie.dropna().values,clean=False, index=True)
    else:
        print(f'{method} is not a valid method, please check the spelling')
    injust = []
    for ii in idx:
        month = dt.datetime(Serie.index[ii].year,Serie.index[ii].month, 1)
        if month not in ENSO.index:
            injust.append(ii)

    if  len(injust) == 0:
        S = Serie
    else:
        S = Serie.drop(Serie.index[injust])
        if write == True:
            outliers = Serie.iloc[injust]
            outliers.to_csv(os.path.join(Path_Out, f'Outliers_{name}_{method}.csv'))
    if graph == True:
        outliers = Serie.iloc[injust]
        GraphSerieOutliersMAD(Serie, outliers,
                              name=f'Outliers_{name}_{method}',
                              label=label,title=title,pdf=pdf, png=png,
                              PathFigs=Path_Out)

    return S
