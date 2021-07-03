#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import numpy as np
import pandas as pd
import datetime as dt

from Modules import Read
from Modules.Utils import Listador
from Modules.FitStats import BestFit, QuantilBestFit
from PDF_fit import OuliersENSOjust
from ENSO import ONIdata

Path_yearly = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Datos/Anuales/'))
Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData'))
Path_out = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Balance'))

Tr = np.array([2.33, 5, 10, 25, 50, 100, 200, 500, 1000])

Magan_m = Read.EstacionCSV_pd('25027680.csv', 'MAGANGUE',    path=Est_path)
Barbo_m = Read.EstacionCSV_pd('25027530.csv', 'BARBOSA',     path=Est_path)
Armen_m = Read.EstacionCSV_pd('25027360.csv', 'ARMENIA',     path=Est_path)
Cruz3_m = Read.EstacionCSV_pd('25027640.csv', 'CRUCES',      path=Est_path)
Monte_m = Read.EstacionCSV_pd('25017010.csv', 'MONTELIBANO', path=Est_path)

Magan_m.index = [dt.datetime.strptime(fecha.strftime("%Y-%m-%d") , "%Y-%d-%m") for fecha in Magan_m.index]
Barbo_m.index = [dt.datetime.strptime(fecha.strftime("%Y-%m-%d") , "%Y-%d-%m") for fecha in Barbo_m.index]
Armen_m.index = [dt.datetime.strptime(fecha.strftime("%Y-%m-%d") , "%Y-%d-%m") for fecha in Armen_m.index]
Cruz3_m.index = [dt.datetime.strptime(fecha.strftime("%Y-%m-%d") , "%Y-%d-%m") for fecha in Cruz3_m.index]
Monte_m.index = [dt.datetime.strptime(fecha.strftime("%Y-%m-%d") , "%Y-%d-%m") for fecha in Monte_m.index]

ONI = ONIdata()
ONI = ONI['Anomalie'].astype(float)
ENSO = ONI[np.where((ONI.values<=-0.5)|(ONI.values>=0.5))[0]]
Magan = OuliersENSOjust(Magan_m, ENSO, method='IQR', lim_inf=0,  write=False)
Barbo = OuliersENSOjust(Barbo_m, ENSO, method='IQR', lim_inf=0,  write=False)
Armen = OuliersENSOjust(Armen_m, ENSO, method='IQR', lim_inf=0,  write=False)
Cruz3 = OuliersENSOjust(Cruz3_m, ENSO, method='IQR', lim_inf=0,  write=False)
Monte = OuliersENSOjust(Monte_m, ENSO, method='IQR', lim_inf=0,  write=False)


Magan_sum = Magan.groupby(lambda y : y.year).sum()
Barbo_sum = Barbo.groupby(lambda y : y.year).sum()
Armen_sum = Armen.groupby(lambda y : y.year).sum()
Cruz3_sum = Cruz3.groupby(lambda y : y.year).sum()
Monte_sum = Monte.groupby(lambda y : y.year).sum()

Magan_max = Magan.groupby(lambda y : y.year).max()
Barbo_max = Barbo.groupby(lambda y : y.year).max()
Armen_max = Armen.groupby(lambda y : y.year).max()
Cruz3_max = Cruz3.groupby(lambda y : y.year).max()
Monte_max = Monte.groupby(lambda y : y.year).max()


Magan_LM, Magan_MEL, Magan_dist = QuantilBestFit(Magan_max.dropna().values, Tr=Tr)
Barbo_LM, Barbo_MEL, Barbo_dist = QuantilBestFit(Barbo_max.dropna().values, Tr=Tr)
Armen_LM, Armen_MEL, Armen_dist = QuantilBestFit(Armen_max.dropna().values, Tr=Tr)
Cruz3_LM, Cruz3_MEL, Cruz3_dist = QuantilBestFit(Cruz3_max.dropna().values, Tr=Tr)
Monte_LM, Monte_MEL, Monte_dist = QuantilBestFit(Monte_max.dropna().values, Tr=Tr)

# Magan_q = np.max([Magan_LM, Magan_MEL], axis=0)
# Barbo_q = np.max([Barbo_LM, Barbo_MEL], axis=0)
# Armen_q = np.max([Armen_LM, Armen_MEL], axis=0)
# Cruz3_q = np.max([Cruz3_LM, Cruz3_MEL], axis=0)
# Monte_q = np.max([Monte_LM, Monte_MEL], axis=0)

SJorg1_MEL = Magan_MEL - Barbo_MEL
SJorg2_MEL = Magan_MEL - Armen_MEL - Cruz3_MEL

SJorg1_LM = Magan_LM - Barbo_LM
SJorg2_LM = Magan_LM - Armen_LM - Cruz3_LM

# SJorg = pd.DataFrame(np.array([SJorg1,SJorg2]).T, index=Tr, columns=['Magangue - Barbosa','Magangue - Armenia - Tres Cruces'])
SJ_MEL = [Magan_MEL,Barbo_MEL,Armen_MEL,Cruz3_MEL,Monte_MEL,SJorg1_MEL,SJorg2_MEL]
SJ_LM  = [Magan_LM ,Barbo_LM ,Armen_LM ,Cruz3_LM ,Monte_LM ,SJorg1_LM ,SJorg2_LM ]

SJorg_MEL = pd.DataFrame(np.array(SJ_MEL).T, index=Tr, columns=['Magangue', 'Barbosa', 'Armenia', 'Tres Cruces', 'Montelibano', 'Magangue - Barbosa','Magangue - Armenia - Tres Cruces'])
SJorg_LM  = pd.DataFrame(np.array(SJ_LM ).T, index=Tr, columns=['Magangue', 'Barbosa', 'Armenia', 'Tres Cruces', 'Montelibano', 'Magangue - Barbosa','Magangue - Armenia - Tres Cruces'])
SJorg_MEL.to_csv(os.path.join(Path_out,'BalanceSanJorgeTr_MEL.csv'))
SJorg_LM .to_csv(os.path.join(Path_out,'BalanceSanJorgeTr_LM.csv'))

import matplotlib
matplotlib.use ("Agg")
import matplotlib.pyplot as plt

rojo    = '#d7191c'
naranja = '#e66101'
verdeos = '#018571'
azul    = '#2c7bb6'
verde   = '#1a9641'
morado  = '#5e3c99'


plt.close('all')
figure =plt.figure(figsize=(7.9,4.8))
ax = figure.add_subplot(1,1,1)

ax.plot(Magan_sum,color=morado, lw=2, label='Magangue')
ax.plot(Barbo_sum,color=naranja,lw=2, label='Barbosa')
ax.plot(Monte_sum,color=azul,   lw=2, label='Montelibano')
ax.plot(Armen_sum,color=rojo,   lw=2, label='Armenia')
ax.plot(Cruz3_sum,color=verde,  lw=2, label='Tres Cruces')
ax.set_ylabel('Anual Flow [m$^{3}$ s$^{-1}$ year]')
ax.set_xlabel('Year')
ax.legend(loc=0)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
plt.savefig(os.path.join(Path_out,'Balance.pdf'),format='pdf', transparent=True)
plt.savefig(os.path.join(Path_out,'Balance.png'),format='png', transparent=True)



plt.close('all')
figure =plt.figure(figsize=(12.8,8))
ax = figure.add_subplot(1,1,1)
ax.plot(Tr, Magan_MEL,color=morado, lw=2, label='Magangue')
ax.plot(Tr, Barbo_MEL,color=naranja,lw=2, label='Barbosa')
ax.plot(Tr, Monte_MEL,color=azul,   lw=2, label='Montelibano')
ax.plot(Tr, Armen_MEL,color=rojo,   lw=2, label='Armenia')
ax.plot(Tr, Cruz3_MEL,color=verde,  lw=2, label='Tres Cruces')
ax.set_xscale('log')
ax.legend(loc=0)
ax.set_xlabel('Return period [years]')
ax.set_ylabel(u'Flow [m$^{3}$ s$^{-1}$]')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
# ax.spines['bottom'].set_visible(False)
# ax.spines['left'].set_visible(False)
plt.savefig(os.path.join(Path_out,'BalanceTr_MEL.pdf'),format='pdf', transparent=True)
plt.savefig(os.path.join(Path_out,'BalanceTr_MEL.png'),format='png', transparent=True)

plt.close('all')
figure =plt.figure(figsize=(12.8,8))
ax = figure.add_subplot(1,1,1)
ax.plot(Tr, Magan_LM,color=morado, lw=2, label='Magangue')
ax.plot(Tr, Barbo_LM,color=naranja,lw=2, label='Barbosa')
ax.plot(Tr, Monte_LM,color=azul,   lw=2, label='Montelibano')
ax.plot(Tr, Armen_LM,color=rojo,   lw=2, label='Armenia')
ax.plot(Tr, Cruz3_LM,color=verde,  lw=2, label='Tres Cruces')
ax.set_xscale('log')
ax.legend(loc=0)
ax.set_xlabel('Return period [years]')
ax.set_ylabel(u'Flow [m$^{3}$ s$^{-1}$]')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
# ax.spines['bottom'].set_visible(False)
# ax.spines['left'].set_visible(False)
plt.savefig(os.path.join(Path_out,'BalanceTr_LM.pdf'),format='pdf', transparent=True)
plt.savefig(os.path.join(Path_out,'BalanceTr_LM.png'),format='png', transparent=True)



plt.close('all')
figure =plt.figure(figsize=(12.8,8))
ax = figure.add_subplot(1,1,1)
ax.plot(Tr, SJorg1_MEL,color=morado, lw=2, label='Magangue - Barbosa')
ax.plot(Tr, SJorg2_MEL,color=rojo,   lw=2, label='Magangue - Armenia - Tres Cruces')
ax.set_xscale('log')
ax.legend(loc=0)
ax.set_xlabel('Return period [years]')
ax.set_ylabel(u'Flow [m$^{3}$ s$^{-1}$]')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
# ax.spines['bottom'].set_visible(False)
# ax.spines['left'].set_visible(False)
plt.savefig(os.path.join(Path_out,'BalanceSanJorgeTr_MEL.pdf'),format='pdf', transparent=True)
plt.savefig(os.path.join(Path_out,'BalanceSanJorgeTr_MEL.png'),format='png', transparent=True)

plt.close('all')
figure =plt.figure(figsize=(12.8,8))
ax = figure.add_subplot(1,1,1)
ax.plot(Tr, SJorg1_LM,color=morado, lw=2, label='Magangue - Barbosa')
ax.plot(Tr, SJorg2_LM,color=rojo,   lw=2, label='Magangue - Armenia - Tres Cruces')
ax.set_xscale('log')
ax.legend(loc=0)
ax.set_xlabel('Return period [years]')
ax.set_ylabel(u'Flow [m$^{3}$ s$^{-1}$]')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
# ax.spines['bottom'].set_visible(False)
# ax.spines['left'].set_visible(False)
plt.savefig(os.path.join(Path_out,'BalanceSanJorgeTr_LM.pdf'),format='pdf', transparent=True)
plt.savefig(os.path.join(Path_out,'BalanceSanJorgeTr_LM.png'),format='png', transparent=True)
