#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import numpy as np
import pandas as pd
import datetime as dt
from Modules import Read
from Modules.Utils import Listador, FindOutlier
from Modules.Graphs import DurationCurve

Path_out = os.path.abspath(os.path.join(os.path.dirname(__file__), 'DurationCurve'))
# Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData'))
Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanNiveles'))

Estaciones = Listador(Est_path,final='.csv')

Pruebas = ['Rachas', 'PuntoCambio', 'Spearman', 'Anderson','MannKendall']
Test = pd.DataFrame([], columns=Pruebas)
Outl = pd.DataFrame([], columns=['outlier_inf','outlier_sup'])

for i in range(len(Estaciones)):

    Meta = pd.read_csv(os.path.join(Est_path, Estaciones[i].split('.')[0]+'.meta'),index_col=0)
    Name = Meta.iloc[0].values[0]

    Dat = Read.EstacionCSV_pd(Estaciones[i], Name, path=Est_path)
    dat =  Dat.values.ravel()
    Percentiles = np.arange(0,101)
    DurationCurve(100-Percentiles, np.percentile(dat,Percentiles),
                  title=Name,
                  label=u'Caudal [m$^{3}$ s$^{-1}$]' if Meta.iloc[-4].values[0]=='CAUDAL' else 'Nivel',
                  png=True, pdf=False,
                  name=Estaciones[i].split('.csv')[0],
                  PathFigs=os.path.join(Path_out,Meta.iloc[-4].values[0]))
