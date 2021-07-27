# -*- coding: utf-8 -*-
"""
Created on Sun Oct 11 18:00:30 2020

@author: Andres
"""
import os
import numpy as np
import pandas as pd
import datetime as dt
import scipy.stats as st
import pylab as plt

from Modules import Read
from Modules.Utils import Listador, FindOutlier
from Modules.FitStats import QuantilBestFit
from Modules.ENSO import ONIdata, OuliersENSOjust

from tqdm import tqdm

ONI = ONIdata()
ONI = ONI['Anomalie'].astype(float)
ENSO = ONI[np.where((ONI.values<=-0.5)|(ONI.values>=0.5))[0]]

################################   INPUT   #####################################

Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData/PPT'))
# Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData/QDL'))
Path_out = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Ajustes'))


Estaciones = Listador(Est_path,final='.csv')

if Est_path.endswith('CleanSedimentos'):
    Path_out = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Sedimentos/Ajustes/'))
    Estaciones = Listador(Est_path, inicio='Trans',final='.csv')


n_boots = int(1E2)
split_fact = 0.8
Tr = np.array([2.33, 5, 10, 25, 50, 100, 200, 500, 1000])
Quant = pd.DataFrame([], columns=Tr)
Uncer = pd.DataFrame([], columns=Tr)

Q_LM = np.empty((len(Estaciones), len(Tr)), dtype=float)
Q_MEL= np.empty((len(Estaciones), len(Tr)), dtype=float)
u_LM = np.empty((len(Estaciones), len(Tr)), dtype=float)
u_MEL= np.empty((len(Estaciones), len(Tr)), dtype=float)
dist = np.empty(len(Estaciones), dtype='<U15')

pbar = tqdm(total=len(Estaciones), desc='Fittin station: ')

for i in range(len(Estaciones)):
    if Est_path.endswith('CleanSedimentos') == False:
        Meta = pd.read_csv(os.path.join(Est_path, Estaciones[i].split('.')[0]+'.meta'),index_col=0)
        Name = Meta.iloc[0].values[0]

        if Est_path.endswith('CleanNiveles'):
            Est = Name + 'NR'
        else:
            Est  = Name+'Caudal' if Meta.iloc[-4].values[0]=='CAUDAL' else Name+'Nivel'

        serie = Read.EstacionCSV_pd(Estaciones[i], Est, path=Est_path)

    else:
        Est  = Estaciones[i].split('_')[1].split('.csv')[0]
        serie = pd.read_csv(os.path.join(Est_path, Estaciones[i]), index_col=0)
        serie.index = pd.DatetimeIndex(serie.index)


    serie = OuliersENSOjust(serie, ENSO, lim_inf=0)

    serie = serie.groupby(lambda y : y.year).max()
    serie = serie[~np.isnan(serie.values)].values.ravel()

    if len(serie)  == 0:
        continue
    try:
        Q_LM[i,:], Q_MEL[i,:], dist[i] = QuantilBestFit(serie, Tr)
    except:
        Q_LM [i,:] *= np.nan
        Q_MEL[i,:] *= np.nan
        dist[i] = 'fit_failure'

    unc_LM  = np.empty((n_boots,len(Tr)),dtype=float)
    unc_MEL = np.empty((n_boots,len(Tr)),dtype=float)

    barra = tqdm(total=n_boots, desc=f'bootstraping {i}/{len(Estaciones)}: ')
    for b in range(n_boots):
        if split_fact*len(serie)<4:
            size = 4
            if len(serie)>10:
                size = int(0.5*len(serie))
        else:
            size = int(split_fact*len(serie))

        sample = np.random.choice(serie, size=size)
        try:
            unc_LM[b,:], unc_MEL[b,:], _ = QuantilBestFit(sample, Tr)
        except:
            unc_LM [b,:] *= np.nan
            unc_MEL[b,:] *= np.nan
        barra.update(1)
    barra.close()

    u_LM [i,:] = np.nanstd(unc_LM,  ddof=1, axis=0)
    u_MEL[i,:] = np.nanstd(unc_MEL, ddof=1, axis=0)

    quant = pd.Series(Q_LM[i],name=Est+'_LM', index=Tr)
    Quant = Quant.append(quant)
    quant = pd.Series(Q_MEL[i],name=Est+'_MEL', index=Tr)
    Quant = Quant.append(quant)

    uncer = pd.Series(u_LM[i],name=Est+'_LM', index=Tr)
    Uncer = Uncer.append(uncer)
    uncer = pd.Series(u_MEL[i],name=Est+'_MEL', index=Tr)
    Uncer = Uncer.append(uncer)

    pbar.update(1)

pbar.close()



if Est_path.endswith('PPT'):
    sufix = 'PPT'
elif Est_path.endswith('QDL'):
    sufix = 'QDL'
else:
    sufix = ''


Quant.to_csv(os.path.join(Path_out,f'CuantilesIncertidumbre_{sufix}.csv'))
Uncer.to_csv(os.path.join(Path_out,f'Incertidumbre_{sufix}.csv'))
