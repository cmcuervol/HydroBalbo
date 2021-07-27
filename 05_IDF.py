# -*- coding: utf-8 -*-
"""
Created on Mon Oct 05 09:54:42 2020

@author: Andres
"""
import os
import numpy as np
import pandas as pd
from Modules.IDF_Func import Wilches
from Modules.IDF_Func import Pulgarin
from Modules.IDF_Func import VargasDiazGranados
from Modules.IDF_Func import IDEAM
from Modules.Utils import Listador
from Modules import Read

from matplotlib import cm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.colors as colors

################################   INPUT IDF   #################################

Path_series = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Datos/Series/'))
Path_IDF    = os.path.abspath(os.path.join(os.path.dirname(__file__), 'IDF'))

def MaxAnual(Esta, Path_series, window=None):
    """
    Read estation to extract the anual max value
    """
    Dat = Read.EstacionCSV_np(Esta, Esta.split('.csv')[0],Path_series)
    if window is not None:
        Dat = Dat.rolling(f'{window}D').sum()
        Dat = Dat.dropna()
    Max = Dat.groupby(lambda y : y.year).max()

    return Max[~np.isnan(Max.values)].values.ravel()/24

def GraphIDF(Int, duration, frecuency, cmap_name='jet', name='IDF', pdf=True, png=False, PathFigs=Path_IDF,):
    """
    Graph of month diurnal cycles

    INPUTS
    Int       : 2D array with the Intesity [mm/hour] with shape=(len(durations), len(frecuency))
    duration  : 1D array with durations [min]
    frecuency : 1D array with reuturn periods [years]
    cmap_name : color map name
    name      : stringo for save the figure
    Path      : abtolute Path to save files
    """

    # define some random data that emulates your indeded code:
    NCURVES = len(frecuency)
    plt.close('all')
    fig = plt.figure(figsize=(9.6,6))
    ax = fig.add_subplot(111)
    cNorm  = colors.Normalize(vmin=0, vmax=NCURVES)
    scalarMap = cm.ScalarMappable(norm=cNorm, cmap=plt.get_cmap(cmap_name))

    lines = []
    for idx in range(NCURVES):
        line = Int[:, idx]
        colorVal  = scalarMap.to_rgba(idx)
        colorText = str(frecuency[idx])+' years'
        retLine,  = ax.plot(duration,line, linewidth=2,
                            color=colorVal,
                            label=colorText)
        lines.append(retLine)
    #added this to get the legend to work
    handles,labels = ax.get_legend_handles_labels()

    # # Shrink current axis by 20%
    # box = ax.get_position()
    # ax.set_position([box.x0, box.y0, box.width*1.0, box.height])
    ax.legend(handles, labels, loc='center right', bbox_to_anchor=(1, 0.5),
              fancybox=False, shadow=False)

    ax.set_xlabel('Duration [minutes]', fontsize=16)
    ax.set_ylabel('Intensity [mm/hour]', fontsize=16)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    # ax.spines['bottom'].set_visible(False)
    # ax.spines['left'].set_visible(False)
    if pdf == True:
        plt.savefig(os.path.join(PathFigs, name+'.pdf'), format='pdf', transparent=True)
        if png == True:
            plt.savefig(os.path.join(PathFigs, name+'.png'), transparent=True)
    elif png == True:
        plt.savefig(os.path.join(PathFigs, name+'.png'), transparent=True)
    else:
        print("Graph not saved. To save it at least one of png or pdf parameters must be True.")


Estaciones = Listador(Path_series, final='.csv')

# Return periods
Tr = np.array([2.33, 5, 10, 25, 50, 100, 200, 500, 1000])
theta = -0.82
#
for i in range(len(Estaciones)):
    Name = Estaciones[i].split('.csv')[0]

    data = MaxAnual(Estaciones[i], Path_series)
    dP, Idq = Pulgarin(data, Tr, theta)
    GraphIDF(Idq, dP, Tr, cmap_name='jet', name=Name+'IDF', pdf=True, png=False, PathFigs=Path_IDF,)
    IDF = pd.DataFrame(Idq, index=dP, columns=Tr)
    IDF.to_csv(os.path.join(Path_IDF,Estaciones[i]))


# Compare Wilches with VargasDiazGranados
Tr = np.array([2,3,5,10,25,50,100])

# Nombre = 'ESPERANZA LA [25021340]'
# Nombre = 'TORNO EL HACIENDA [25021470]'
Nombre = 'CARMEN DE BOLIVAR [29015020]'

# IDEAM params
C1 = [6798.308,9998.576,14882.323,23468.705,39184.485,55085.160,75025.218]
X0 = [27.895,32.735,37.828,43.764,50.544,55.091,59.234]
C2 = [1.112,1.159,1.207,1.263,1.325,1.366,1.403]

# data = MaxAnual(Nombre+'.csv', Path_series)
#
# from math import factorial as fact
# x = np.sort(data)
# n = len(x)
# ks = np.array([0, 1])
# Mk = np.zeros(len(ks))
#
# for j in range(len(ks)):
#     k = ks[j]
#     N = fact(n-1)/(fact(k)*fact(n-1-k))
#     Mi = 0.
#     for i in range(n-k):
#         Ni = fact(n-(i+1))/(fact(k)*fact(n-(i+1)-k))
#         # print (Ni)
#         Mi = x[i]*Ni
#         Mk[j] = Mk[j] + Mi
#     Mk[j] = (1./n)*Mk[j]/N
#
# alphad = (Mk[0] - 2.*Mk[1])/np.log(2.)
# mud = Mk[0] - 0.5772*alphad
#
# y = -np.log(-np.log(1.-1./Tr))
#
# ################################   Duration   ##############################
#
# dlim1 = 1440
# dlim2 = 60
# dlim3 = 5
# d1 = np.arange(dlim2, dlim1, 5)
# d2 = np.arange(dlim3, ddlim2, 2.5)
#
# # IDF curves
# Idq1 = np.zeros((len(d1), len(Tr)))
# Idq2 = np.zeros((len(d2), len(Tr)))
#
# for i in range(len(Tr)):
#     Idq1[:,i] = (mud + alphad*y[i])*(d1/1440.)**theta
#     Idq2[:,i] = Idq1[0,i]*((32.4/d2**0.75) - (30.00/d2))
#
# d = np.hstack((d2, d1))
# Idq = np.vstack((Idq2, Idq1))
#
# X1 = Idq1[0,:]
# X2 = mud + alphad*y

for w in np.arange(1,8):
    data = MaxAnual(Nombre+'.csv', Path_series, window=w)
    Name = f'{Nombre}_{w}days_'
    dP, Idq = Pulgarin(data, Tr, theta)
    dV, IdV = VargasDiazGranados(data, Tr, Region=2)
    dI, Ida = IDEAM(Tr, X0,C1,C2)
    DiP = Ida-Idq
    DiV = Ida-IdV

    GraphIDF(Idq, dP, Tr, cmap_name='jet', name=Name+'IDF_Pulgarin',   pdf=True, png=False, PathFigs=Path_IDF,)
    GraphIDF(IdV, dV, Tr, cmap_name='jet', name=Name+'IDF_Vargas',     pdf=True, png=False, PathFigs=Path_IDF,)
    GraphIDF(Ida, dI, Tr, cmap_name='jet', name=Name+'IDF_IDEAM',      pdf=True, png=False, PathFigs=Path_IDF,)
    GraphIDF(DiV, dV, Tr, cmap_name='jet', name=Name+'IDF_DifVargas',  pdf=True, png=False, PathFigs=Path_IDF,)
    GraphIDF(DiP, dV, Tr, cmap_name='jet', name=Name+'IDF_DifPulgarin',pdf=True, png=False, PathFigs=Path_IDF,)

    IDF_P = pd.DataFrame(Idq, index=dP, columns=Tr)
    IDF_V = pd.DataFrame(IdV, index=dV, columns=Tr)
    IDF_A = pd.DataFrame(Ida, index=dI, columns=Tr)
    IDF_p = pd.DataFrame(DiP, index=dP, columns=Tr)
    IDF_v = pd.DataFrame(DiV, index=dV, columns=Tr)

    IDF_P.to_csv(os.path.join(Path_IDF,Name+'_Purlgarin.csv'))
    IDF_V.to_csv(os.path.join(Path_IDF,Name+'_Vargas.csv'))
    IDF_A.to_csv(os.path.join(Path_IDF,Name+'_IDEAM.csv'))
    IDF_p.to_csv(os.path.join(Path_IDF,Name+'_DifVargas.csv'))
    IDF_v.to_csv(os.path.join(Path_IDF,Name+'_DifPulgarin.csv'))


    cmap_name = 'jet'
    pdf = True
    png = False
    PathFigs = Path_IDF
    name=Name+'IDF_all'
    # define some random data that emulates your indeded code:
    NCURVES = len(Tr)
    plt.close('all')
    fig = plt.figure(figsize=(9.6,6))
    ax = fig.add_subplot(111)
    cNorm  = colors.Normalize(vmin=0, vmax=NCURVES)
    scalarMap = cm.ScalarMappable(norm=cNorm, cmap=plt.get_cmap(cmap_name))

    lines = []
    for idx in range(NCURVES):
        line = Ida[:, idx]
        colorVal  = scalarMap.to_rgba(idx)
        colorText = f'IDEAM    {Tr[idx]} years'
        retLine,  = ax.plot(dI,line, linewidth=2,
                            color=colorVal,
                            label=colorText)
        lines.append(retLine)

        ax.scatter(dV, IdV[:, idx], marker='*', s=10 ,c=colorVal, label=f'Pulgarin {Tr[idx]} years')
        ax.scatter(dP, Idq[:, idx], marker='o', s=10 ,c=colorVal, label=f'Vargas   {Tr[idx]} years')
    #added this to get the legend to work
    handles,labels = ax.get_legend_handles_labels()

    # # Shrink current axis by 20%
    # box = ax.get_position()
    # ax.set_position([box.x0, box.y0, box.width*1.0, box.height])
    ax.legend(handles, labels, loc='center right', bbox_to_anchor=(1, 0.5),
              fancybox=False, shadow=False)

    ax.set_xlabel('Duration [minutes]', fontsize=16)
    ax.set_ylabel('Intensity [mm/hour]', fontsize=16)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    # ax.spines['bottom'].set_visible(False)
    # ax.spines['left'].set_visible(False)
    if pdf == True:
        plt.savefig(os.path.join(PathFigs, name+'.pdf'), format='pdf', transparent=True)
        if png == True:
            plt.savefig(os.path.join(PathFigs, name+'.png'), transparent=True)
    elif png == True:
        plt.savefig(os.path.join(PathFigs, name+'.png'), transparent=True)
    else:
        print("Graph not saved. To save it at least one of png or pdf parameters must be True.")
