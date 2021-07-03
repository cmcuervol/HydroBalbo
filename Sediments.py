#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import numpy as np
import pandas as pd
import datetime as dt
from scipy import stats
import pymannkendall as mk

from Modules import Read
from Modules.Utils import Listador, FindOutlier, Cycles
from Modules.Graphs import GraphSerieOutliers, GraphDataFrames, GraphSingleDF
from TestRandomnes import RunsTest,ChangePointTest,SpearmanCoefTest,AndersonTest,MannKendall_modified


Path_out = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Sedimentos'))
Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanSedimentos'))


Res_est = Listador(Est_path, inicio='resolidos', final='.csv')
Trn_est = Listador(Est_path, inicio='Trans',     final='.csv')
Men_est = Listador(Est_path, inicio='Valores_',  final='.csv')

Armen_r = pd.read_csv(os.path.join(Est_path, Res_est[0]), index_col=0)
Coque_r = pd.read_csv(os.path.join(Est_path, Res_est[1]), index_col=0)
Esper_r = pd.read_csv(os.path.join(Est_path, Res_est[2]), index_col=0)
Rayar_r = pd.read_csv(os.path.join(Est_path, Res_est[3]), index_col=0)
Magan_r = pd.read_csv(os.path.join(Est_path, Res_est[4]), index_col=0)
Monte_r = pd.read_csv(os.path.join(Est_path, Res_est[5]), index_col=0)
Palen_r = pd.read_csv(os.path.join(Est_path, Res_est[6]), index_col=0)
Armen_r.index = pd.DatetimeIndex(Armen_r.index)
Coque_r.index = pd.DatetimeIndex(Coque_r.index)
Esper_r.index = pd.DatetimeIndex(Esper_r.index)
Rayar_r.index = pd.DatetimeIndex(Rayar_r.index)
Magan_r.index = pd.DatetimeIndex(Magan_r.index)
Monte_r.index = pd.DatetimeIndex(Monte_r.index)
Palen_r.index = pd.DatetimeIndex(Palen_r.index)

Armen_t = pd.read_csv(os.path.join(Est_path, Trn_est[0]), index_col=0)
Coque_t = pd.read_csv(os.path.join(Est_path, Trn_est[1]), index_col=0)
Esper_t = pd.read_csv(os.path.join(Est_path, Trn_est[2]), index_col=0)
Rayar_t = pd.read_csv(os.path.join(Est_path, Trn_est[3]), index_col=0)
Magan_t = pd.read_csv(os.path.join(Est_path, Trn_est[4]), index_col=0)
Monte_t = pd.read_csv(os.path.join(Est_path, Trn_est[5]), index_col=0)
Palen_t = pd.read_csv(os.path.join(Est_path, Trn_est[6]), index_col=0)
Armen_t.index = pd.DatetimeIndex(Armen_t.index)
Coque_t.index = pd.DatetimeIndex(Coque_t.index)
Esper_t.index = pd.DatetimeIndex(Esper_t.index)
Rayar_t.index = pd.DatetimeIndex(Rayar_t.index)
Magan_t.index = pd.DatetimeIndex(Magan_t.index)
Monte_t.index = pd.DatetimeIndex(Monte_t.index)
Palen_t.index = pd.DatetimeIndex(Palen_t.index)

Armen_m = pd.read_csv(os.path.join(Est_path, Men_est[0]), index_col=0)
Coque_m = pd.read_csv(os.path.join(Est_path, Men_est[1]), index_col=0)
Esper_m = pd.read_csv(os.path.join(Est_path, Men_est[2]), index_col=0)
Rayar_m = pd.read_csv(os.path.join(Est_path, Men_est[3]), index_col=0)
Magan_m = pd.read_csv(os.path.join(Est_path, Men_est[4]), index_col=0)
Monte_m = pd.read_csv(os.path.join(Est_path, Men_est[5]), index_col=0)
Palen_m = pd.read_csv(os.path.join(Est_path, Men_est[6]), index_col=0)
Armen_m.index = pd.DatetimeIndex(Armen_m.index)
Coque_m.index = pd.DatetimeIndex(Coque_m.index)
Esper_m.index = pd.DatetimeIndex(Esper_m.index)
Rayar_m.index = pd.DatetimeIndex(Rayar_m.index)
Magan_m.index = pd.DatetimeIndex(Magan_m.index)
Monte_m.index = pd.DatetimeIndex(Monte_m.index)
Palen_m.index = pd.DatetimeIndex(Palen_m.index)

names = ['Armernia',
         'La Coquera',
         'La Esperanza',
         'La Raya',
         'Magangué',
         'Montelibano',
         'Palenquito']

Df_r = [Armen_r, Coque_r, Esper_r, Rayar_r, Magan_r, Monte_r, Palen_r]
Df_t = [Armen_t, Coque_t, Esper_t, Rayar_t, Magan_t, Monte_t, Palen_t]
Df_m = [Armen_m, Coque_m, Esper_m, Rayar_m, Magan_m, Monte_m, Palen_m]

GraphDataFrames(Df_r, names, col=0,label='Nivel [cm]',
                name='Niveles', pdf=False, png=True,PathFigs=Path_out)
GraphDataFrames(Df_r, names, col=1,label=u'Caudal liquido [m$^{3}$ s$^{-1}$]',
                name='Caudal',pdf=False, png=True,PathFigs=Path_out)
GraphDataFrames(Df_r, names, col=2, label= 'Gasto solido [kg s$^{-1}$]',
                name='Gasto',pdf=False, png=True,PathFigs=Path_out)
GraphDataFrames(Df_r, names, col=3, label=u'Concetración media [kg m$^{-3}$]',
                name='Concentra_med',pdf=False, png=True,PathFigs=Path_out)
GraphDataFrames(Df_r, names, col=4, label=u'Concetración superficial [kg m$^{-3}$]',
                name='Concentra_sup',pdf=False, png=True,PathFigs=Path_out)

for i in range(len(Df_t)):
    GraphSingleDF(Df_t[i], title=names[i],
                  label='MATERIALES EN SUSPENSION  [K.TON/DIA]',
                  name=f"Sed_diurnal_{names[i].replace(' ','')}",
                  pdf=False, png=True,PathFigs=Path_out)

    GraphSingleDF(Df_t[i], title=names[i],
                  label='VALORES TOTALES MENSUALES DE TRANSPORTE [K.TON/DIA]',
                  name=f"Sed_monthly_{names[i].replace(' ','')}",
                  pdf=False, png=True,PathFigs=Path_out)



# =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
# =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
#                                   TestRandomnes
# =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
# =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*

Path_test = os.path.join(Path_out,'Test')
Pruebas = ['Rachas', 'PuntoCambio', 'Spearman', 'Anderson','MannKendall']
Test = pd.DataFrame([], columns=Pruebas)
Outl = pd.DataFrame([], columns=['outlier_inf','outlier_sup'])

for i in range(len(Df_t)):

    yearly  = Df_t[i].groupby(lambda y: y.year).max().values.ravel()
    mensual = Df_t[i].groupby(lambda m: (m.year,m.month)).max()
    out_inf, out_sup = FindOutlier(mensual,clean=False,index=False,lims=True, restrict_inf=0)
    # Path_SaveFigure  = os.path.join(Path_out,Meta.iloc[-4].values[0])
    GraphSerieOutliers(mensual, out_inf, out_sup,
                       title=names[i],
                       label='MATERIALES EN SUSPENSION  [K.TON/DIA]',
                       png=True, pdf=False,
                       name=f"Outliers_{names[i].replace(' ','')}",
                       PathFigs=Path_test)
    if len(yearly)>3:
        tst = {'Rachas'     :RunsTest(yearly),
               'PuntoCambio':ChangePointTest(yearly),
               'Spearman'   :SpearmanCoefTest(yearly),
               'Anderson'   :AndersonTest(yearly),
               'MannKendall':MannKendall_modified(yearly, rezagos=None),}
        out = {'outlier_inf':out_inf,
               'outlier_sup':out_sup}

        Est = pd.Series(data=tst, name=names[i])
        Out = pd.Series(data=out, name=names[i])
        Test = Test.append(Est)
        Outl = Outl.append(Out)

Test.to_csv(os.path.join(Path_test,'Test_sed.csv'),     sep=',')
Outl.to_csv(os.path.join(Path_test,'Outliers_sed.csv'), sep=',')
