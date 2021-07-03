# -*- coding: utf-8 -*-
"""
Created on Mon Oct 05 18:50:42 2020

@author: Andres
"""
import os
import pandas as pd
import numpy as np
import pylab as plt
import datetime as dt

from Modules import Read
from Modules.Utils import Listador
# Pruebas tendencia
from Modules.Homogeneidad import mk_test, T_trend
# Pruebas cambio varianza
from Modules.Homogeneidad import FSimple, FMod, Bartlett, AnsariBradley, Levene
# Pruebas cambio media
from Modules.Homogeneidad import TSimple, TMod, UMann, KruskallWallis

################################   INPUT   #####################################
Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData'))
# Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanNiveles'))
# Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanSedimentos'))
Path_out = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Tests/Homogeneidad'))

Estaciones = Listador(Est_path,final='.csv')

if Est_path.endswith('CleanSedimentos'):
    Path_out = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Sedimentos/Ajustes/'))
    Estaciones = Listador(Est_path, inicio='Trans',final='.csv')

pruebas_media = ['T-M', 'T-S', 'M-W', 'K-W']
pruebas_var   = ['F-M', 'F-S', 'A-B', 'B', 'L']
pruebas_tend  = ['MK-test', 'T-test']

Res_Med  = pd.DataFrame([], columns=pruebas_media)
Res_std  = pd.DataFrame([], columns=pruebas_var)
Res_tend = pd.DataFrame([], columns=pruebas_tend)

for i in range(len(Estaciones)):
    if Est_path.endswith('CleanSedimentos') == False:
        Meta = pd.read_csv(os.path.join(Est_path, Estaciones[i].split('.')[0]+'.meta'),index_col=0)
        Name = Meta.iloc[0].values[0]
        if Est_path.endswith('CleanNiveles'):
            Est = Name + 'NR'
            unidades = '[m]'
            label = 'Nivel [m]'
        else:
            Est  = Name+'Caudal' if Meta.iloc[-4].values[0]=='CAUDAL' else Name+'Nivel'
            unidades = '[m$^{3}$ s$^{-1}$]' if Meta.iloc[-4].values[0]=='CAUDAL' else '[cm]'
            label = 'Caudal '+ unidades if Meta.iloc[-4].values[0]=='CAUDAL' else 'Nivel '+unidades

        data = Read.EstacionCSV_pd(Estaciones[i], Est, path=Est_path)
        if Est_path.endswith('CleanNiveles'):
            data.index = [dt.datetime.strptime(fecha.strftime("%Y-%m-%d") , "%Y-%d-%m") for fecha in data.index]
    else:
        Est  = Estaciones[i].split('_')[1].split('.csv')[0]
        data = pd.read_csv(os.path.join(Est_path, Estaciones[i]), index_col=0)
        data.index = pd.DatetimeIndex(data.index)
        mensual = data.groupby(lambda m: (m.year,m.month)).max()
        mensual.index = [dt.datetime(t[0],t[1],1) for t in mensual.index]
        data = mensual
        unidades = '[kTon/dia]'
        label = 'Trans [kTon/dia]'

    try:
        data = data.dropna()
        data = data.sort_index()
        # Est = 'SUCRE [25027110]'
        # data = pd.read_csv(Est + '.csv', index_col = 0, header = None)
        dates = data.index
        times = plt.date2num(dates)
        # times = [plt.date2num(dt.datetime.strptime(d,'%Y-%m-%d')) for d in dates]
        # dates = [dt.datetime.strptime(d,'%Y-%m-%d') for d in dates]
        serie = np.asarray(data.iloc[:,0])

        alpha = 0.01
        ventana = 12*5


        ################################################################################
        ##########################   PRUEBAS DE TENDENCIA   ############################
        ################################################################################

        # MANN KENDALL
        serie_trend = serie[~np.isnan(serie)]
        hMK, p = mk_test(serie_trend, alpha)

        # T
        hTR = T_trend(serie_trend, alpha)

        ################################################################################
        #####################   PRUEBAS CAMBIO MEDIA Y VARIANZA   ######################
        ################################################################################

        med = np.zeros((len(serie), 4))*np.NaN
        var = np.zeros((len(serie), 5))*np.NaN
        S1 = []
        S2 = []
        M1 = []
        M2 = []
        FS = []
        FM = []
        FS_crit = []
        FM_crit = []
        TS = []
        TM = []
        TScrit = []
        TMcrit = []

        for j in range(ventana, len(serie)-ventana):

            # Series a comparar
            serie1 = serie[:j]
            serie2 = serie[j:]
            serie1 = serie1[~np.isnan(serie1)]
            serie2 = serie2[~np.isnan(serie2)]

            # Parametros requeridos F Simple-Test
            s1 = np.std(serie1, ddof = 1)
            s2 = np.std(serie2, ddof = 1)
            S1.append(s1)
            S2.append(s2)
            M1.append(np.mean(serie1))
            M2.append(np.mean(serie2))

            # Cambio en la varianza
            hFS, fs = FSimple(serie1, serie2, alpha)
            hFM, fm = FMod(serie1, serie2, alpha)
            hAB = AnsariBradley(serie1, serie2, alpha)
            hB = Bartlett(serie1, serie2, alpha)
            hL = Levene(serie1, serie2, alpha)

            # Cambio en la media
            hTS, ts = TSimple(serie1, serie2, alpha, hFS)
            hTM, tm = TMod(serie1, serie2, alpha, hFM)
            hUM = UMann(serie1, serie2, alpha)
            hKW = KruskallWallis(serie1, serie2, alpha)

            var[j,:] = np.array([hFM, hFS, hAB, hB, hL])
            med[j,:] = np.array([hTM, hTS, hUM, hKW])

        #    FS.append(fs)
        #    FM.append(fm)
        #
        #    TS.append(ts)
        #    TM.append(tm)



        ################################   FIGURE   ####################################

        import matplotlib
        import matplotlib.dates as mdates
        #import matplotlib.ticker as ticker
        matplotlib.rc('text', usetex = False)
        matplotlib.rcParams['mathtext.fontset'] = 'stix'
        #matplotlib.rcParams['font.family'] = 'STIXGeneral'
        font = {'family':'serif', 'serif': ['Times New Roman']}
        plt.rc('font', **font)
        matplotlib.rcParams['text.latex.preamble']=[r'\usepackage{amsmath}']

        # Start figure and subfigures
        from matplotlib import gridspec
        fig1 = plt.figure(figsize=(7, 7))
        gs = gridspec.GridSpec(5, 1, height_ratios = [0.8,0.6,0.6,1,1])
        gs.update(left = 0.132, right = 0.98, top = 0.965, bottom = 0.05, hspace=0.12, wspace = 0.04)

        # Plot parameters
        tick_spacing = 12*5
        fontsize = 16
        ymin = np.nanmin(serie)-10
        ymax = np.nanmax(serie)+10
        cmap = matplotlib.colors.ListedColormap(['grey','red'])

        ###############################   Time series   ################################

        ax0 = plt.subplot(gs[0])
        plt.plot(times, serie, 'o-', ms = 4, mfc = 'w', mec = 'k', color = 'k', lw = 2)
        plt.ylim([ymin, ymax])
        plt.ylabel(label, fontsize = fontsize, labelpad = 0)
        #ax0.xaxis.set_major_locator(mdates.DayLocator(bymonthday=(1), interval=1))
        plt.tick_params(axis='x', which='both', labelsize=fontsize)
        plt.tick_params(axis='y', which='both', labelsize=fontsize)
        #ax0.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
        #xfmt = mdates.DateFormatter('%Y')
        years = mdates.YearLocator(4)
        years_fmt = mdates.DateFormatter('%Y')
        months = mdates.MonthLocator()
        # format the ticks
        ax0.xaxis.set_major_locator(years)
        ax0.xaxis.set_major_formatter(years_fmt)
        #ax0.xaxis.set_major_locator(mdates.YearLocator(2))
        #ax0.xaxis.set_minor_locator(months)
        #ax0.xaxis.set_major_formatter(xfmt)
        plt.title(Est, fontsize = fontsize+2)
        ax0.tick_params('both', length=5, width = 1.5, which='major', direction = 'in')
        #ax0.tick_params('both', length=3, width = 1.2, which='minor')
        ax0.set_xlim((np.min(times), np.max(times)))
        plt.setp(ax0.get_xticklabels(), visible=False)
        #plt.setp(ax0.get_yticklabels(), visible=False)
        for axis in ['top','bottom','left','right']:
             ax0.spines[axis].set_linewidth(1.5)


        ###############################   Time series   ################################

        times_hog = times[ventana:len(serie)-ventana]
        ax0A = plt.subplot(gs[1], sharex = ax0)
        plt.plot(times_hog, M1, color = 'b', lw = 2, label = 'anterior')
        plt.plot(times_hog, M2, color = 'r', lw = 2, label = 'posterior')
        plt.ylim([ymin, ymax])
        plt.ylabel('Media \n '+unidades, fontsize = fontsize, labelpad = 0)
        #ax0.xaxis.set_major_locator(mdates.DayLocator(bymonthday=(1), interval=1))
        plt.tick_params(axis='x', which='both', labelsize=fontsize)
        plt.tick_params(axis='y', which='both', labelsize=fontsize)
        # format the ticks
        ax0A.xaxis.set_major_locator(years)
        ax0A.xaxis.set_major_formatter(years_fmt)
        #ax0A.xaxis.set_major_locator(mdates.YearLocator(2))
        #ax0A.xaxis.set_minor_locator(months)
        #ax0.xaxis.set_major_formatter(xfmt)
        ax0A.tick_params('both', length=5, width = 1.5, which='major', direction = 'in')
        #ax0A.tick_params('both', length=3, width = 1.2, which='minor')
        ax0A.set_xlim((np.min(times), np.max(times)))
        plt.setp(ax0A.get_xticklabels(), visible=False)
        #plt.setp(ax0.get_yticklabels(), visible=False)
        for axis in ['top','bottom','left','right']:
             ax0A.spines[axis].set_linewidth(1.5)

        ax0A.legend(loc = 'center', bbox_to_anchor=(0.5, 0.8), ncol = 2, columnspacing=0.5,
                   handletextpad=0.1, numpoints = 1, handlelength=1.5, fontsize = fontsize-1,
                   scatterpoints = 1, frameon = False)


        times_hog = times[ventana:len(serie)-ventana]
        ax0B = plt.subplot(gs[2], sharex = ax0)
        plt.plot(times_hog, S1, color = 'b', lw = 2, label = 'anterior')
        plt.plot(times_hog, S2, color = 'r', lw = 2, label = 'posterior')
        #ax0.xaxis.set_major_locator(mdates.DayLocator(bymonthday=(1), interval=1))
        plt.tick_params(axis='x', which='both', labelsize=fontsize)
        plt.tick_params(axis='y', which='both', labelsize=fontsize)
        plt.ylabel(u'Desviación \n'+unidades, fontsize = fontsize, labelpad = 0)
        # format the ticks
        ax0B.xaxis.set_major_locator(years)
        ax0B.xaxis.set_major_formatter(years_fmt)
        #ax0B.xaxis.set_major_locator(mdates.YearLocator(2))
        #ax0B.xaxis.set_minor_locator(months)
        #ax0.xaxis.set_major_formatter(xfmt)
        ax0B.tick_params('both', length=5, width = 1.5, which='major', direction = 'in')
        #ax0B.tick_params('both', length=3, width = 1.2, which='minor')
        ax0B.set_xlim((np.min(times), np.max(times)))
        plt.setp(ax0B.get_xticklabels(), visible=False)
        #plt.setp(ax0.get_yticklabels(), visible=False)
        for axis in ['top','bottom','left','right']:
             ax0B.spines[axis].set_linewidth(1.5)

        ax0B.legend(loc = 'center', bbox_to_anchor=(0.5, 0.8), ncol = 2, columnspacing=0.5,
                   handletextpad=0.1, numpoints = 1, handlelength=1.5, fontsize = fontsize,
                   scatterpoints = 1, frameon = False)


        ###############################   Cambio media   ###############################

        times_hog = times[ventana:len(serie)-ventana]
        med = med[ventana:len(serie)-ventana]
        x, y = np.meshgrid(np.asarray(times_hog), np.arange(1,6))

        ax1 = plt.subplot(gs[3], sharex = ax0)
        c = ax1.pcolor(x, y, med.T, cmap = cmap, vmin = 0, vmax = 1)#,
        #               edgecolor = 'k', linewidths = 0.8)
        ax1.set_yticks([2, 3, 4], minor=True)
        ax1.yaxis.grid(True, which='minor', color = 'k', linestyle = '-', linewidth = 1)
        plt.ylim([1,5])
        plt.yticks([1.5, 2.5, 3.5, 4.5], pruebas_media)
        plt.tick_params(axis='x', which='both', labelsize=fontsize)
        plt.tick_params(axis='y', which='both', labelsize=fontsize)
        plt.ylabel('Pruebas media', fontsize = fontsize)
        # format the ticks
        ax1.xaxis.set_major_locator(years)
        ax1.xaxis.set_major_formatter(years_fmt)
        #ax1.xaxis.set_major_locator(mdates.YearLocator(2))
        #ax1.xaxis.set_minor_locator(months)
        #ax0.xaxis.set_major_formatter(xfmt)
        ax1.tick_params('both', length=5, width = 1.5, which='major', direction = 'in')
        #ax1.tick_params('both', length=3, width = 1.2, which='minor')
        #ax1.tick_params('both', length=5, width = 1.5, which='major')
        ax1.set_xlim((np.min(times), np.max(times)))
        plt.setp(ax1.get_xticklabels(), visible=False)
        #plt.setp(ax0.get_yticklabels(), visible=False)
        for axis in ['top','bottom','left','right']:
             ax1.spines[axis].set_linewidth(1.5)

        #plt.colorbar(c)

        ###############################   Cambio varianza   ############################

        var = var[ventana:len(serie)-ventana]
        x, y = np.meshgrid(np.asarray(times_hog), np.arange(1,7))
        ax2 = plt.subplot(gs[4], sharex = ax1)
        c = ax2.pcolor(x, y, var.T, cmap = cmap, vmin = 0, vmax = 1)#,
        #               edgecolor = 'k', linewidths = 1)
        #plt.scatter(x[:-1,:-1]+0.5,y[:-1,:-1]+0.5, color = 'blue')
        ax2.set_yticks([2, 3, 4, 5], minor=True)
        ax2.yaxis.grid(True, which='minor', color = 'k', linestyle = '-', linewidth = 1)
        plt.ylim([1,6])
        plt.yticks([1.5, 2.5, 3.5, 4.5, 5.5], pruebas_var)
        plt.tick_params(axis='x', which='both', labelsize=fontsize)
        plt.tick_params(axis='y', which='both', labelsize=fontsize)
        plt.ylabel('Pruebas varianza', fontsize = fontsize)
        # format the ticks
        ax2.xaxis.set_major_locator(years)
        ax2.xaxis.set_major_formatter(years_fmt)
        #ax2.xaxis.set_major_locator(mdates.YearLocator(2))
        #ax2.xaxis.set_minor_locator(months)
        #ax0.xaxis.set_major_formatter(xfmt)
        ax2.tick_params('both', length=5, width = 1.5, which='major', direction = 'in')
        #ax2.tick_params('both', length=3, width = 1.2, which='minor')
        ax2.set_xlim((np.min(times), np.max(times)))
        #plt.setp(ax2.get_xticklabels(), visible=False)
        #plt.setp(ax0.get_yticklabels(), visible=False)
        for axis in ['top','bottom','left','right']:
             ax2.spines[axis].set_linewidth(1.5)

        import matplotlib.patches as mpatches
        red_patch = mpatches.Patch(color='red', label='cambio')
        gray_patch = mpatches.Patch(color='grey', label='no cambio')
        ax2.legend(handles=[red_patch, gray_patch], fontsize = fontsize)

        plt.savefig(os.path.join(Path_out,'HOMG_' + Est + '.png'), dpi = 400)


        Med = pd.Series(np.sum(med, axis=0),name=Est, index=pruebas_media)
        Res_Med = Res_Med.append(Med)
        std = pd.Series(np.sum(var, axis=0),name=Est, index=pruebas_var)
        Res_std = Res_std.append(std)
        tend = pd.Series(np.array([hMK,hTR]),name=Est, index=pruebas_tend)
        Res_tend = Res_tend.append(tend)

        if Est_path.endswith('CleanNiveles'):
            sufix = 'NR'
        elif Est_path.endswith('CleanSedimentos'):
            sufix = 'Sed'
        else:
            sufix = ''
    except:
        continue

Res_Med .to_csv(os.path.join(Path_out,f'ResumenMedia_{sufix}.csv'))
Res_std .to_csv(os.path.join(Path_out,f'ResumenVarianza_{sufix}.csv'))
Res_tend.to_csv(os.path.join(Path_out,f'ResumenTendencia_{sufix}.csv'))
