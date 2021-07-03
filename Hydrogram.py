#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import numpy as np
import pandas as pd
import datetime as dt

from Modules import Read
from Modules.FitStats import BestFit, QuantilBestFit
from Modules.Utils import Listador, Salto, HistogramValues
from Modules.Graphs import GraphHistrogram, GraphEvents, GraphScatter

Path_yearly = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Datos/Anuales/'))
Path_out    = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Hydrograms'))
Est_path    = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData'))


Estaciones = Listador(Path_yearly, final='csv')

Q_LM = np.empty(len(Estaciones), dtype=float)
Q_MEL= np.empty(len(Estaciones), dtype=float)
dist = np.empty(len(Estaciones), dtype='<U15')

# ['mean', 'max', 'min', 'sum', 'count']

# Factor = [1.37, 1, 2.86, 1.88, 2.24,1,1.66,4.03]
Hydrogram = []
MaxHydro  = []
for i in range(len(Estaciones)):

    S_yearly = pd.read_csv(os.path.join(Path_yearly,Estaciones[i]), index_col = 0, header = 0)
    # S_yearly = S_yearly * Factor[i]
    Q_LM[i], Q_MEL[i], dist[i] = QuantilBestFit(S_yearly['max'].dropna().values, Tr=2.33)

    Q = np.max((Q_LM[i], Q_MEL[i]))

    code = Estaciones[i].split('[')[-1].split(']')[0]
    S_diurnal = Read.EstacionCSV_pd(code+'.csv', Estaciones[i].split('.csv')[0], Est_path)
    # S_diurnal = S_diurnal.sort_index
    # S_diurnal = S_diurnal * Factor[i]
    idh = np.where(S_diurnal.values >Q)[0] # superan umbral

    ide = Salto(idh, 1, condition='>') #identificar eventos
    ide = np.insert(ide, 0, 0) # add 0 to make it easy
    evn = []
    Qp_dur = []
    Qp_vol = []
    q = []
    for e in range(len(ide)-1):
        Event = S_diurnal.iloc[idh[ide[e]:ide[e+1]]]
        q.append(Event.values.ravel())
        evn.append([Event.mean() .values[0],
                    Event.max()  .values[0],
                    Event.min()  .values[0],
                    Event.sum()  .values[0],
                    Event.count().values[0],
                    ])
        Qp_dur.append(Event.max().values[0]/Event.count().values[0])
        Qp_vol.append(Event.max().values[0]/Event.sum()  .values[0])

    evn = np.array(evn)
    idmax = np.where(evn[:,1] == np.max(evn[:,1]))[0][0]  # max Qp
    Hydro = pd.DataFrame(q[idmax], columns=['caudal'])
    Hydro.to_csv(os.path.join(Path_out,'Hydrogram_'+Estaciones[i].replace(' ', '')))

    tp = np.where(q[idmax]==np.max(q[idmax]))[0][0]*24
    maxh = [Estaciones[i].split('.csv')[0],   # Name
            evn[idmax,1],                     # Peak
            tp,                               # Peak time
            evn[idmax,4],                     # Duration
            evn[idmax,3],                     # Volumen
            ]
    MaxHydro.append(maxh)

    GraphHistrogram(Qp_dur,bins=20,
                    label=u'$\mathrm{Q_p/duration}$ [m$^{3}$ s$^{-1}$ day$^{-1}$]',
                    title=Estaciones[i].split('.csv')[0],
                    name='Peak_duration_'+Estaciones[i].replace(' ', '').split('.csv')[0],
                    PathFigs=Path_out )
    GraphHistrogram(Qp_vol,bins=20,
                    label=u'$\mathrm{Q_p/Vol}$ [day s$^{-1}$]',
                    title=Estaciones[i].split('.csv')[0],
                    name='Peak_vol_'+Estaciones[i].replace(' ', '').split('.csv')[0],
                    PathFigs=Path_out )

    GraphEvents(q,Unitarian=False,
                name='Events_'+Estaciones[i].replace(' ', '').split('.csv')[0],
                PathFigs=Path_out )
    GraphEvents(q,Unitarian=True,
                name='EventsUnit_'+Estaciones[i].replace(' ', '').split('.csv')[0],
                PathFigs=Path_out )

    GraphScatter(evn[:,1],evn[:,3],
                 xlabel='Caudal pico [m$^{3}$ s$^{-1}$]',
                 ylabel='Volumen [m$^{3}$]',
                 title=Estaciones[i].split('.csv')[0],
                 name='ScatterQV_'+Estaciones[i].replace(' ', '').split('.csv')[0],
                 pdf=False, png=True, PathFigs=Path_out,)
    GraphScatter(evn[:,3],evn[:,4],
                 xlabel='Volumen [m$^{3}$]',
                 ylabel='Duración creciente [dias]',
                 title=Estaciones[i].split('.csv')[0],
                 name='ScatterVD_'+Estaciones[i].replace(' ', '').split('.csv')[0],
                 pdf=False, png=True, PathFigs=Path_out,)

    Hydrogram.append(evn)
Params = pd.DataFrame(MaxHydro, columns=['Basin', 'Peak', 'PeakTime [hours]', 'Duration [days]', 'Volumen'])
Params.to_csv(os.path.join(Path_out,'HydrogramParams.csv'))
