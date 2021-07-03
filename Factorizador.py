#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import numpy as np
import pandas as pd
import datetime as dt
from scipy import stats
# from Utils import Listador
from Modules import Read
from tqdm import  tqdm


Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData'))
Est_Nivl = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanNiveles'))

# Process the raw data
# Read.SplitAllIDEAM(Depatamento='NivelesAll', sept=';')
# Read.SplitAllIDEAM(Depatamento='CaudalesAll',sept=',')


def ReturnPeriod(serie, value):
    """
    Calculate the return period of a value in a serie
    INPUTS
    serie : pandas series with the data
    value : float to cacule the return period
    """
    # idx = np.where((~np.isnan(serie.values))&(serie.values>=value))[0]
    # if len(idx) == 0: # is a NaN value
    #     return np.nan
    # else:
    #     # excedence = len(idx)/(len(serie)/12.)
    #     return 1/excedence

    p = stats.exponweib.cdf(value, *stats.exponweib.fit(serie.iloc[np.where(~np.isnan(serie))[0]].values.ravel(), 0, 1,))
    return 1./(1.-p)

def FindQPR(serie, events, lag=3, group='mean'):
    """
    Find flow and return period in some events
    INPUTS
    serie  : pandas series with the data
    events : indexes or dates of the events
    lag    : integer of days before to search
    group  : kind of agrupation in the lag
    """
    q  = np.zeros(len(events))
    pr = np.zeros(len(events))
    for i in range(len(events)):
        day = np.where(serie.index == events[i])[0]
        if len(day) != 0:
            ide = day[0]
            if group == 'mean':
                q [i] = np.nanmean(serie.iloc[ide-lag:ide+1].values)
            elif group == 'max':
                q [i] = np.nanmax(serie.iloc[ide-lag:ide+1].values)
            elif group == 'min':
                q [i] = np.nanmin(serie.iloc[ide-lag:ide+1].values)
            elif group == 'sum':
                q [i] = np.nansum(serie.iloc[ide-lag:ide+1].values)
            pr[i] = ReturnPeriod(serie, q[i])
        else:
            q [i] = np.nan
            pr[i] = np.nan

    Q  = pd.DataFrame(q,  index=events, columns=serie.columns)
    PR = pd.DataFrame(pr, index=events, columns=serie.columns)
    return Q, PR

#series to use

# Magan_m = Read.EstacionCSV_np('MAGANGUE [25027680].csv',           'MAGANGUE',    path=Est_path)
# Barbo_m = Read.EstacionCSV_np('BARBOSA [25027530].csv',            'BARBOSA',     path=Est_path)
# Armen_m = Read.EstacionCSV_np('ARMENIA [25027360].csv',            'ARMENIA',     path=Est_path)
# Cruz3_m = Read.EstacionCSV_np('TRES CRUCES [25027640].csv',        'CRUCES',      path=Est_path)
# Nechi_m = Read.EstacionCSV_np('LA ESPERANZA NECHI [27037010].csv', 'NECHI',       path=Est_path)
# Monte_m = Read.EstacionCSV_np('MONTELIBANO - AUT [25017010].csv',  'MONTELIBANO', path=Est_path)
# Coque_m = Read.EstacionCSV_np('LA COQUERA - AUT [26247020].csv',   'COQUERA',     path=Est_path)
#
# Beirt_m = Read.EstacionCSV_np('BEIRUT - AUT [25027760].csv',     'BEIRUT', path=Est_path)
# Marco_m = Read.EstacionCSV_np('SAN MARCOS - AUT [25027220].csv', 'MARCOS', path=Est_path)
# Jegua_m = Read.EstacionCSV_np('JEGUA [25027240].csv',            'JEGUA',  path=Est_path)

# Anton = pd.read_csv(os.path.join(Path_n, 'Bolivar_NivMax_Mes.csv'),sep=';')
# Anton = pd.DataFrame(Anton['Valor'].values, index=pd.DatetimeIndex(Anton['Fecha'],dayfirst=True), columns=['ANTONIO'])
#
Magan_d = Read.EstacionCSV_pd('25027680.csv', 'MAGANGUE',    path=Est_path)
Barbo_d = Read.EstacionCSV_pd('25027530.csv', 'BARBOSA',     path=Est_path)
Coyog_d = Read.EstacionCSV_pd('25027930.csv', 'COYOGAL',     path=Est_path)
Armen_d = Read.EstacionCSV_pd('25027360.csv', 'ARMENIA',     path=Est_path)
Cruz3_d = Read.EstacionCSV_pd('25027640.csv', 'CRUCES',      path=Est_path)
Nechi_d = Read.EstacionCSV_pd('27037010.csv', 'NECHI',       path=Est_path)
Monte_d = Read.EstacionCSV_pd('25017010.csv', 'MONTELIBANO', path=Est_path)
Coque_d = Read.EstacionCSV_pd('26247020.csv', 'COQUERA',     path=Est_path)
Rayal_d = Read.EstacionCSV_pd('25027910.csv', 'RAYA',        path=Est_path)
Varas_d = Read.EstacionCSV_pd('25027200.csv', 'VARAS',       path=Est_path)

Beirt_d = Read.EstacionCSV_pd('25027760NR.csv', 'BEIRUT', path=Est_Nivl)
Marco_d = Read.EstacionCSV_pd('25027220NR.csv', 'MARCOS', path=Est_Nivl)
Jegua_d = Read.EstacionCSV_pd('25027240NR.csv', 'JEGUA',  path=Est_Nivl)
Anton_d = Read.EstacionCSV_pd('25027180NR.csv', 'ANTONIO',path=Est_Nivl)

# eventos
idx   = np.where((~np.isnan(Magan_d.values))&(Magan_d.values>=10000))[0]
dates = Magan_d.iloc[idx].index



# series = [Armen, Cruz3, Nechi, Monte, Coque, Barbo, Magan, Beirt, Marco, Jegua, Anton]
# series = [Armen, Cruz3, Nechi, Monte, Coque, Barbo, Magan, Beirt, Marco, Jegua]

series = [Armen_d, Cruz3_d, Nechi_d, Monte_d, Coque_d, Barbo_d, Magan_d, Coyog_d, Rayal_d, Varas_d, Beirt_d, Marco_d, Anton_d, Jegua_d]
pbar = tqdm(total=len(series), desc='Calculating return periods: ')
for i in range(len(series)):
    q, pr = FindQPR(series[i], events=dates, lag=5, group='max')
    if i == 0:
        Q  = q
        PR = pr
    else:
        Q  = Q .join(q,  how='inner')
        PR = PR.join(pr, how='inner')
    pbar.update(1)
pbar.close()

# Q = Q.join(Magan.iloc[idx], how='inner')

Q .to_csv(os.path.join(os.getcwd(), 'CaudalesEvents_5daysMax_.csv'), sep=',')
PR.to_csv(os.path.join(os.getcwd(), 'ReturnPeriodEvents_5daysMax_.csv'), sep=',')


# data = Magan.values
#
# import matplotlib.pyplot as plt
# # plt.plot(data[~np.isnan(data)], stats.exponweib.pdf(data[~np.isnan(data)], *stats.exponweib.fit(data, 1, 1, scale=0.2, loc=0)),'.')
# plt.plot(data[~np.isnan(data)], stats.exponweib.pdf(data[~np.isnan(data)], *stats.exponweib.fit(data[~np.isnan(data)], 2, 1, loc=0)),'.')
# _ = plt.hist(data[~np.isnan(data)], bins=np.linspace(np.nanmin(data), np.nanmax(data),50), normed=True, alpha=0.5);
# plt.show()
#
# p = stats.exponweib.cdf(11100, *stats.exponweib.fit(data[~np.isnan(data)], 0, 1,))
