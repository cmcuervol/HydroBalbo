#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import numpy as np
import pandas as pd
import datetime as dt
from scipy import stats
import pymannkendall as mk

from Modules import Read
from Modules.Utils import Listador, FindOutlier, FindOutlierMAD, Cycles
from Modules.Graphs import GraphSerieOutliers, GraphSerieOutliersMAD
from ENSO import ONIdata


ONI = ONIdata()
ONI = ONI['Anomalie'].astype(float)
ENSO = ONI[np.where((ONI.values<=-0.5)|(ONI.values>=0.5))[0]]

def OuliersENSOjust(Serie, ENSO=ENSO, method='IQR', lim_inf=0,
                    write=True, name=None,
                    graph=True, label='', title='', pdf=False, png=True):
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
            outliers.to_csv(os.path.join(Path_out, f'Outliers_{name}_{method}.csv'))
    if graph == True:
        outliers = Serie.iloc[injust]
        GraphSerieOutliersMAD(Serie, outliers,
                              name=f'Outliers_{name}_{method}',
                              label=label,title=title,pdf=pdf, png=png,
                              PathFigs=Path_out)

    return S


Path_out = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Tests'))

def CompareNormStandar(Statistical, significance, tails=1):
    """
    Compare an statistical of any test with the normal standar
    INPUTS
    Statistical : float of the value to compare in the normal standar
    significance: level of confidence to acept o reject the test
    tails       : integer in [1,2] to use a test with one or two tails
    OUTPUTS
    test : boolean with the aceptance of rejection of the null hypothesis
    """
    cuantil = 1-significance/tails
    Z_norm  = stats.norm.ppf(cuantil,loc=0,scale=1)
    Pass    = abs(Statistical)<Z_norm
    return Pass

def CompareTdist(Statistical, DegreesFredom, significance, tails=1):
    """
    Compare an statistical of any test with the t estudent distribution
    INPUTS
    Statistical   : float of the value to compare in the normal standar
    DegreesFredom : Degrees of fredom of the distirbution
    significance  : level of confidence to acept o reject the test
    tails         : integer in [1,2] to use a test with one or two tails
    OUTPUTS
    test : boolean with the aceptance of rejection of the null hypothesis
    """
    cuantil = 1-significance/tails
    t = stats.t.ppf(cuantil,df=DegreesFredom)
    Pass    = abs(Statistical)<t
    return Pass

def SingChange(Serie):
    """
    Count times where are sing change
    INPUTS
    Serie : list or array of with the data
    """
    if isinstance(Serie, list) == True:
        Serie = np.array(Serie)

    sing = np.zeros(len(Serie),dtype=int) +1
    sing[np.array(Serie)<0] = -1
    # return sum((x ^ y)<0 for x, y in zip(Serie, Serie[1:])) # only works for integers
    return sum((x ^ y)<0 for x, y in zip(sing, sing[1:]))


def PeaksValleys(Serie):
    """
    Fin the peaks and valleys in a serie
    INPUTS
    Serie : list or array of with the data
    """
    if isinstance(Serie, list) == True:
        Serie = np.array(Serie)

    diff = Serie[:-1]-Serie[1:]

    sing = np.zeros(len(diff),dtype=int) +1
    sing[np.array(diff)<0] = -1

    return sum(((x ^ y)^(y ^ z))<0 for x, y, z in zip(sing, sing[1:], sing[2:]))


def RunsTest(Serie, significance=5E-2):
    """
    Make  run test (Rachas) for a series
    INPUTS
    Serie : list or array with the data
    significance : level of significance to acept or reject the null hypothesis
    OUTPUTS
    test : boolean with the aceptance of rejection of the null hypothesis
    """
    S_median = np.median(Serie)
    runs = SingChange(Serie-S_median)
    n1 = np.where(Serie>=S_median)[0].shape[0]
    n2 = np.where(Serie< S_median)[0].shape[0]

    runs_exp = ((2*n1*n2)/(n1+n2))+1
    stan_dev = np.sqrt((2*n1*n2*(2*n1*n2-n1-n2))/ \
                       (((n1+n2)**2)*(n1+n2-1)))

    z = (runs-runs_exp)/stan_dev

    test = CompareNormStandar(z, significance,tails=2)

    return test


def ChangePointTest(Serie, significance=5E-2):
    """
    Make change point test for a serie
    INPUTS
    Serie : list or array with the data
    significance : level of significance to acept or reject the null hypothesis
    OUTPUTS
    test : boolean with the aceptance of rejection of the null hypothesis
    """
    N = len(Serie)
    M = PeaksValleys(Serie)

    U = abs((M-(2./3.)*(N-2))/np.sqrt((16*N-29)/90.))

    test = CompareNormStandar(U, significance,tails=2)
    return test

def SpearmanCoefTest(Serie, significance=5E-2):
    """
    Make Spearman coeficient test
    INPUTS
    Serie : list or array with the data
    significance : level of significance to acept or reject the null hypothesis
    OUTPUTS
    test : boolean with the aceptance of rejection of the null hypothesis
    """

    if isinstance(Serie, list) == True:
        Serie = np.array(Serie)

    n = len(Serie)
    S = Serie[Serie.argsort()]

    R = 1-(6/(n*((n**2)-1)))* np.sum((Serie-S)**2 )

    U = abs(R*np.sqrt(n-2)/np.sqrt(1-(R**2)))

    test = CompareTdist(U,DegreesFredom=n-2,significance=significance,tails=2)
    return test



def AndersonTest(Serie, rezagos=None, significance=5E-2, ):
    """
    Make andreson independence test
    INPUTS

    """

    cuantil = 1-significance/2
    Z_norm  = stats.norm.ppf(cuantil,loc=0,scale=1)
    N = len(Serie)
    if rezagos is None:
        rezagos = N -2
    Mean = np.nanmean(Serie)
    r = np.empty(len(Serie), dtype=float)
    t = np.empty(len(Serie), dtype=bool)

    for k in range(rezagos):
        lim_up = (-1 + Z_norm*np.sqrt(N-k-1))/(N-k)
        lim_dw = (-1 - Z_norm*np.sqrt(N-k-1))/(N-k)
        r[k] =  np.sum((Serie[:N-k]-Mean)*(Serie[k:]-Mean))/np.sum((Serie - Mean)**2)
        if (r[k] > lim_dw)&(r[k]<lim_up):
            t[k] = True
        else:
            t[k] = False
    if t.sum() == N:
        test == True
    else:
        test = False

    return test

def MannKendall_modified(Serie, rezagos=None, significance=5E-2):
    """
    This function checks the Modified Mann-Kendall (MK) test using Hamed and Rao (1998) method.
    """
    MK  = mk.hamed_rao_modification_test(Serie,alpha=significance,lag=rezagos)
    test = CompareNormStandar(MK.z, significance,tails=2)
    return test


Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData'))
# Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanNiveles'))


Estaciones = Listador(Est_path,final='.csv')

if Est_path.endswith('CleanData') == True:
    Estaciones = ['25017010.csv']
elif Est_path.endswith('CleanNiveles') == True:
    Estaciones = ['25017010NR.csv']

Pruebas = ['Rachas', 'PuntoCambio', 'Spearman', 'Anderson','MannKendall']
Test = pd.DataFrame([], columns=Pruebas)
Outl = pd.DataFrame([], columns=['outlier_inf','outlier_sup'])

for i in range(len(Estaciones)):

    Meta = pd.read_csv(os.path.join(Est_path, Estaciones[i].split('.')[0]+'.meta'),index_col=0)
    if Meta.iloc[-2].values[0] == u'Nivel máximo diario':
        continue

    Name = Meta.iloc[0].values[0]
    Esta  = Estaciones[i].split('.csv')[0]
    if Est_path.endswith('CleanNiveles'):
        Meta.iloc[-2].values[0] = u'Nivel máximo diario'
        Name += 'NR_'
        Esta += 'NR'

    Dat = Read.EstacionCSV_pd(Estaciones[i], Name, path=Est_path)
    # if Estaciones[i].endswith('N.csv') == False:
    try:
        Dat.index = [dt.datetime.strptime(fecha.strftime("%Y-%m-%d") , "%Y-%d-%m") for fecha in Dat.index]
    except:
        pass

    Dat = Dat['1994-01-01':]
    Dat = Dat.dropna()
    # dat =  Dat.values.ravel()
    # yearly  = Dat.groupby(lambda y: y.year).max().values.ravel()
    mensual = Dat.groupby(lambda m: (m.year,m.month)).max()
    out_inf, out_sup = FindOutlier(mensual,clean=False,index=False,lims=True, restrict_inf=0)
    # Path_SaveFigure  = os.path.join(Path_out,Meta.iloc[-4].values[0])
    GraphSerieOutliers(mensual, out_inf, out_sup,
                       title=Name,
                       label=Meta.iloc[-2].values[0],
                       png=True, pdf=False,
                       name=Estaciones[i].split('.csv')[0],
                       PathFigs=os.path.join(Path_out,Meta.iloc[-4].values[0]))
    Serie = OuliersENSOjust(Dat.sort_index(),method='IQR', ENSO=ENSO,
                            write=True, name=Esta,
                            graph=True, label=Meta.iloc[-2].values[0], title=Name,
                            pdf=False, png=True)
    serie = OuliersENSOjust(Dat.sort_index(),method='MAD', ENSO=ENSO,
                            write=True, name=Esta,
                            graph=True, label=Meta.iloc[-2].values[0], title=Name,
                            pdf=False, png=True)

    yearly  = Serie.groupby(lambda y: y.year).max().values.ravel()
    if len(yearly)>3:
        tst = {'Rachas'     :RunsTest(yearly),
               'PuntoCambio':ChangePointTest(yearly),
               'Spearman'   :SpearmanCoefTest(yearly),
               'Anderson'   :AndersonTest(yearly),
               'MannKendall':MannKendall_modified(yearly, rezagos=None),}
        out = {'outlier_inf':out_inf,
               'outlier_sup':out_sup}

        Est = pd.Series(data=tst, name=Name+'Caudal' if Meta.iloc[-4].values[0]=='CAUDAL' else Name+'Nivel')
        Out = pd.Series(data=out, name=Name+'Caudal' if Meta.iloc[-4].values[0]=='CAUDAL' else Name+'Nivel')
        Test = Test.append(Est)
        Outl = Outl.append(Out)

if Est_path.endswith('CleanNiveles'):
    sufix = 'NR'
else:
    sufix = ''
Test.to_csv(os.path.join(Path_out,f'Test_{sufix}.csv'),     sep=',')
Outl.to_csv(os.path.join(Path_out,f'Outliers_{sufix}.csv'), sep=',')
