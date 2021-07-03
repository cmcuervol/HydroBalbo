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
from Modules.Utils import Listador, FindOutlier, FindOutlierMAD
from ENSO import ONIdata
############################   L-moments functions   ###########################

from Modules.Lmoments import Lmom, Lmom1
from Modules.Lmoments import L_ratiodiagram
from Modules.Lmoments import Lnorm, Lexp, Lgumbel, LGPA, LGEV, LGLO, LLOG3, LP3
from Modules.Lmoments import EXPcdf, GUMcdf, NORMcdf, GPAcdf, GEVcdf, GLOcdf, LLOG3cdf, LP3cdf
from Modules.Lmoments import EXPq, GUMq, NORMq, GPAq, GEVq, GLOq, LLOG3q, LP3q

dist_names = ['norm', 'expon', 'gumbel_r', 'genpareto',
              'genextreme', 'genlogistic', 'lognorm', 'pearson3']
names  = ['Norm.', 'Exp.', 'Gumbel', 'GPA', 'GEV',  'GLO',  'LOGN3', 'P3']
colors = ['k'   ,   'g' ,   'gold',   'b',   'r', 'lime', 'orange',  'm']

Ldist = [Lnorm, Lexp, Lgumbel, LGPA, LGEV, LGLO, LLOG3, LP3]
LCDF  = [NORMcdf, EXPcdf, GUMcdf, GPAcdf, GEVcdf, GLOcdf, LLOG3cdf, LP3cdf]
Lq    = [NORMq, EXPq, GUMq, GPAq, GEVq, GLOq, LLOG3q, LP3q]


################################   INPUT   #####################################

Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData'))
# Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanNiveles'))
# Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanSedimentos'))
Path_out = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Ajustes'))

# Read.SplitAllIDEAM('NivelReal', Est_path=Est_path,Nivel=True)
ONI = ONIdata()
ONI = ONI['Anomalie'].astype(float)
ENSO = ONI[np.where((ONI.values<=-0.5)|(ONI.values>=0.5))[0]]

def OuliersENSOjust(Serie, ENSO=ENSO, method='IQR', lim_inf=0, write=True, name=None, Path_out=os.getcwd()):
    """
    Remove  outliers with the function find outliers and justify the values in ENSO periods
    INPUTS
    Serie : Pandas DataFrame or pandas Series with index as datetime
    ENSO  : Pandas DataFrame with the index of dates of ENSO periods
    method: str to indicate the mehtod to find outliers, ['IQR','MAD']
    lim_inf : limit at the bottom for the outliers
    write : boolean to write the outliers
    Name  : string of estation name to save the outliers
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
    return S

Estaciones = Listador(Est_path,final='.csv')

if Est_path.endswith('CleanSedimentos'):
    Path_out = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Sedimentos/Ajustes/'))
    Estaciones = Listador(Est_path, inicio='Trans',final='.csv')

# idx = np.where(np.array(Estaciones) == '25027220N.csv')[0]
# for i in idx:
Tr = np.array([2.33, 5, 10, 25, 50, 100, 200, 500, 1000])
KT  = ((6**0.5)/np.pi)*(0.5772+ np.log(np.log(Tr/(Tr-1))))
confidence = 0.95
q = 1. - 1./Tr
Resumen = pd.DataFrame([], columns=Tr)
SK_resm = pd.DataFrame([])
# Intrval = pd.DataFrame([], columns=['Inferior', 'Superior'])
Bandas  = pd.DataFrame([], columns=np.append(Tr, Tr))
# Alpha_inverval = 0.95

for i in range(len(Estaciones)):
    if Est_path.endswith('CleanSedimentos') == False:
        Meta = pd.read_csv(os.path.join(Est_path, Estaciones[i].split('.')[0]+'.meta'),index_col=0)
        Name = Meta.iloc[0].values[0]

        if Est_path.endswith('CleanNiveles'):
            Est = Name + 'NR'
        else:
            Est  = Name+'Caudal' if Meta.iloc[-4].values[0]=='CAUDAL' else Name+'Nivel'

        label = u'Caudal [m$^{3}$/s]' if Meta.iloc[-4].values[0]=='CAUDAL' else 'Nivel'
        serie = Read.EstacionCSV_pd(Estaciones[i], Est, path=Est_path)
        try:
            serie.index = [dt.datetime.strptime(fecha.strftime("%Y-%m-%d") , "%Y-%d-%m") for fecha in serie.index]
        except:
            pass
    else:
        Est  = Estaciones[i].split('_')[1].split('.csv')[0]
        label = 'Transporte [KTon/dia]'
        serie = pd.read_csv(os.path.join(Est_path, Estaciones[i]), index_col=0)
        serie.index = pd.DatetimeIndex(serie.index)

    serie = OuliersENSOjust(serie, ENSO, method='IQR', lim_inf=0,  write=True, name=Est, Path_out=Path_out)
    SERIE = OuliersENSOjust(serie, ENSO, method='MAD', lim_inf=0,  write=True, name=Est, Path_out=Path_out)

    serie = serie.groupby(lambda y : y.year).max()
    serie = serie[~np.isnan(serie.values)].values.ravel()

    try:
        lmom, lmomA   = Lmom(serie)
        lmom1, t3, t4 = Lmom1(serie)

        L_ratiodiagram(lmom, Est, PathFigs=Path_out)
    except:
        continue

    ################################   FIGURE   ####################################
    # try:
    import matplotlib
    matplotlib.rc('text', usetex = False)
    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    #matplotlib.rcParams['font.family'] = 'STIXGeneral'
    font = {'family':'serif', 'serif': ['Times New Roman']}
    plt.rc('font', **font)
    matplotlib.rcParams['text.latex.preamble']=[r'\usepackage{amsmath}']

    # Start figure and subfigures
    from matplotlib import gridspec
    plt.close('all')
    fig = plt.figure(figsize=(12, 6))
    gs = gridspec.GridSpec(1, 2)
    gs.update(left = 0.05, right = 0.98, top = 0.95, bottom = 0.11, hspace=0.12, wspace = 0.04)

    # Plot parameters
    fontsize = 17

    #################################   L-Moments   ################################

    paramsLM  = np.zeros((len(dist_names), 3))*np.NaN
    ks_LM     = np.zeros(len(dist_names))
    pvalue_LM = np.zeros(len(dist_names))

    ax1 = plt.subplot(gs[1])
    plt.hist(serie, bins = 30, density = True, alpha = 0.5, cumulative = True)
    # ax1.set_ylabel('CDF', fontsize = fontsize, labelpad = 0)
    # ax1.set_xlabel('Caudal [m$^3$/s]', fontsize = fontsize, labelpad = 0)
    plt.title('L-momentos', fontsize = fontsize)


    x = np.linspace(np.min(serie), np.max(serie), 100)

    for i in range(len(dist_names)):
        dist_name = dist_names[i]
        dist = getattr(st, dist_name)

        if i <= 2:
            Lfunc = Ldist[i]
            loc, scale = Lfunc(lmom)
            cdfi = LCDF[i]
            cdf_fit = cdfi(x, loc, scale)
            cdf = lambda x: cdfi(x, loc, scale)
            ks_LM[i] = st.kstest(serie, cdf)[0]
            pvalue_LM[i] = st.kstest(serie, cdf)[1]
            paramsLM[i][:] = np.array([loc, scale, np.NaN])
        else:
            try:
                Lfunc = Ldist[i]
                loc, scale, shape = Lfunc(lmom)
                cdfi = LCDF[i]
                cdf_fit = cdfi(x, loc, scale, shape)
                cdf = lambda x: cdfi(x, loc, scale, shape)
                ks_LM[i] = st.kstest(serie, cdf)[0]
                pvalue_LM[i] = st.kstest(serie, cdf)[1]
                paramsLM[i][:] = np.array([loc, scale, shape])
            except:
                continue

        plt.plot(x, cdf_fit, lw = 2, color = colors[i], label = names[i])

    plt.xlim([np.min(serie), np.max(serie)])
    plt.ylim([0,1])
    plt.tick_params(axis='x', which='both', labelsize=fontsize, direction = 'in')
    plt.tick_params(axis='y', which='both', labelsize=fontsize, direction = 'in')
    ax1.tick_params('both', length=5, width = 1.5, which='major')
    plt.setp(ax1.get_yticklabels(), visible=False)
    for axis in ['top','bottom','left','right']:
         ax1.spines[axis].set_linewidth(1.8)
    #plt.legend(loc='best')

    ###########################   Maximum Likelihood   #############################

    paramsMEL  = np.zeros((len(dist_names), 3))
    ks_MEL     = np.zeros(len(dist_names))
    pvalue_MEL = np.zeros(len(dist_names))

    ax0 = plt.subplot(gs[0], sharex = ax1)
    plt.hist(serie, bins = 30, density = True, alpha = 0.5, cumulative = True, label = 'Mediciones')
    ax0.set_ylabel('CDF', fontsize = fontsize, labelpad = 0)
    ax0.set_xlabel(label,
                   fontsize = fontsize, labelpad = 0, x = 1.05)

    plt.title(u'Máxima verosimilitud', fontsize = fontsize)
    x = np.linspace(np.min(serie), np.max(serie), 100)
    for i in range(len(dist_names)):
        dist_name = dist_names[i]
        dist = getattr(st, dist_name)
        param = dist.fit(serie, loc = paramsLM[i][0], scale = paramsLM[i][1])
        if i <= 2:
            loc, scale = param
            paramsMEL[i][:2] = np.array([loc, scale])
        else:
            shape, loc, scale = param
            paramsMEL[i][:] = np.array([loc, scale, shape])
        cdf_fit = dist.cdf(x, *param[:-2], loc = param[-2], scale = param[-1])
        plt.plot(x, cdf_fit, lw = 2, color = colors[i], label = names[i])

        # K-S test
        ks_MEL[i] = st.kstest(serie, dist_name, param)[0]
        pvalue_MEL[i] = st.kstest(serie, dist_name, param)[1]

    plt.xlim([np.min(serie), np.max(serie)])
    plt.ylim([0,1])
    plt.tick_params(axis='x', which='both', labelsize=fontsize, direction = 'in')
    plt.tick_params(axis='y', which='both', labelsize=fontsize, direction = 'in')
    ax0.tick_params('both', length=5, width = 1.5, which='major')
    # plt.setp(ax0.get_xticklabels(), visible=False)
    for axis in ['top','bottom','left','right']:
         ax0.spines[axis].set_linewidth(1.8)
    plt.legend(loc='best', fontsize = fontsize-1)

    plt.savefig(os.path.join(Path_out,'Ajustes_' + Est + '.png'), dpi = 400)

    df = pd.DataFrame({'ks_MEL': ks_MEL, 'ks_LM': ks_LM}, index = names)
    df.to_excel(os.path.join(Path_out,'Smirnov-Kolmogorov'+Est+'.xls'))


    ################################################################################
    ##############################   Best distribution   ###########################
    ################################################################################

    best_LM  = np.where(pvalue_LM==np.nanmax(pvalue_LM))[0][0]
    best_MEL = np.where(pvalue_LM==np.nanmax(pvalue_LM))[0][0]
    if best_LM == best_MEL:
        index = best_LM
    else:
        print(f"Choose between index {best_LM} and {best_MEL}")

    # twoparams = 1
    locMEL, scaleMEL, shapeMEL = paramsMEL[index,:]
    locLM,  scaleLM,  shapeLM  = paramsLM [index,:]
    distMEL = getattr(st, dist_names[index])
    distLM  = LCDF[index]

    try:
        cdf_fitMEL = distMEL.cdf(x, locMEL, scaleMEL)
        cdf_fitLM  = distLM(x, locLM, scaleLM)
    except:
        cdf_fitMEL = distMEL.cdf(x, shapeMEL, locMEL, scaleMEL)
        cdf_fitLM  = distLM(x, locLM, scaleLM, shapeLM)

    plt.close('all')
    fig = plt.figure(figsize=(7, 5))
    gs = gridspec.GridSpec(1, 1)
    gs.update(left = 0.10, right = 0.96, top = 0.98, bottom = 0.13, hspace=0.12, wspace = 0.04)

    # Plot parameters
    fontsize = 15
    ax0 = plt.subplot(gs[0])
    plt.hist(serie, bins = 30, density = True, alpha = 0.5, cumulative = True, label = 'Mediciones')
    plt.plot(x, cdf_fitMEL, lw = 2, color = 'b', label = u'Máxima verosimilitud')
    plt.plot(x, cdf_fitLM, lw = 2, color = 'r', label = u'L-momentos')
    plt.xlim([np.min(serie), np.max(serie)])
    plt.ylim([0,1])
    plt.ylabel('CDF', fontsize = fontsize, labelpad = 0)
    plt.xlabel(label,
               fontsize = fontsize, labelpad = 0)
    # plt.xlabel('Nivel [m.s.n.m]', fontsize = fontsize, labelpad = 0)
    plt.tick_params(axis='x', which='both', labelsize=fontsize, direction = 'in')
    plt.tick_params(axis='y', which='both', labelsize=fontsize, direction = 'in')
    ax0.tick_params('both', length=5, width = 1.5, which='major')
    #plt.setp(ax0.get_xticklabels(), visible=False)
    for axis in ['top','bottom','left','right']:
         ax0.spines[axis].set_linewidth(1.8)
    plt.legend(loc='lower right', fontsize = fontsize-1)

    plt.savefig(os.path.join(Path_out,'Best_fit_' + Est + '.png'), dpi = 400)

    # L  moments
    plt.close('all')
    fig = plt.figure(figsize=(7, 5))
    gs = gridspec.GridSpec(1, 1)
    gs.update(left = 0.10, right = 0.96, top = 0.98, bottom = 0.13, hspace=0.12, wspace = 0.04)

    # Plot parameters
    fontsize = 15
    ax0 = plt.subplot(gs[0])
    plt.hist(serie, bins = 30, density = True, alpha = 0.5, cumulative = True, label = 'Mediciones')
    plt.plot(x, cdf_fitLM, lw = 2, color = 'r', label = u'Ajuste  mejor FDP')
    plt.xlim([np.min(serie), np.max(serie)])
    plt.ylim([0,1])
    plt.ylabel('CDF', fontsize = fontsize, labelpad = 0)
    plt.xlabel(label,
               fontsize = fontsize, labelpad = 0)
    # plt.xlabel('Nivel [m.s.n.m]', fontsize = fontsize, labelpad = 0)
    plt.tick_params(axis='x', which='both', labelsize=fontsize, direction = 'in')
    plt.tick_params(axis='y', which='both', labelsize=fontsize, direction = 'in')
    ax0.tick_params('both', length=5, width = 1.5, which='major')
    #plt.setp(ax0.get_xticklabels(), visible=False)
    for axis in ['top','bottom','left','right']:
         ax0.spines[axis].set_linewidth(1.8)
    plt.legend(loc='lower right', fontsize = fontsize-1)

    plt.savefig(os.path.join(Path_out,'Best_fit_LM' + Est + '.png'), dpi = 400)


    ################################################################################
    ######################   Quantiles by best distribution   ######################
    ################################################################################

    best_LM  = np.where(pvalue_LM==np.nanmax(pvalue_LM))[0][0]
    best_MEL = np.where(pvalue_LM==np.nanmax(pvalue_LM))[0][0]
    if best_LM == best_MEL:
        index = best_LM
    else:
        print(f"Choose between index {best_LM} and {best_MEL}")

    # twoparams = 1
    locMEL, scaleMEL, shapeMEL = paramsMEL[index,:]
    locLM, scaleLM, shapeLM = paramsLM[index,:]
    distMEL = getattr(st, dist_names[index])
    distLM  = Lq[index]

    try:
        # Quantiles MEL
        quant_MEL = distMEL.ppf(q, loc = locMEL, scale = scaleMEL)
        # Quantiles LM
        quant_LM = distLM(q, locLM, scaleLM)
        # confidence interval
        # inf, sup  = distMEL.interval(Alpha_inverval, loc = locMEL, scale = scaleMEL)

    except:
        # Quantiles MEL
        quant_MEL = distMEL.ppf(q, shapeMEL, loc = locMEL, scale = scaleMEL)
        # Quantiles LM
        quant_LM = distLM(q, locLM, scaleLM, shapeLM)
        # confidence interval
        # inf, sup  = distMEL.interval(Alpha_inverval, shapeMEL, loc = locMEL, scale = scaleMEL)


    # confidence interval
    sigma = np.std(serie,  ddof=1)
    # se    = st.sem(serie)
    se = 1
    h = KT * sigma * se * st.t.ppf(1 - confidence/2., len(serie)-1)

    inf_MEL = quant_MEL - abs(h)
    sup_MEL = quant_MEL + abs(h)
    inf_LM  = quant_LM  - abs(h)
    sup_LM  = quant_LM  + abs(h)

    ####################   FIGURE
    plt.close('all')
    fig = plt.figure(figsize=(7, 5))
    gs = gridspec.GridSpec(1, 1)
    gs.update(left = 0.11, right = 0.96, top = 0.97, bottom = 0.13, hspace=0.12, wspace = 0.04)

    # Plot parameters
    fontsize = 15
    ymin = 0.0
    ymax = 0.5
    xmin = 0.0
    xmax = 0.5

    # Trpadec = np.array([2, 5, 10, 25, 50, 100, 500, 1000])
    # Qpadec = np.array([2465, 3280, 3820, 4508, 5018, 5524, 6695, 7198])

    ax0 = plt.subplot(gs[0])
    plt.plot(Tr, quant_MEL, 'o-', mfc = 'r', mec = 'k', mew = 1.2, color = 'r',
             lw = 2.5, label = u'Máxima verosimilitud', clip_on = False, zorder = 4)
    plt.plot(Tr, inf_MEL, '--', mfc = 'r', mec = 'k', mew = 1.2, color = 'r',
            lw = 2.5, label = u'Máxima verosimilitud bandas de confianza', clip_on = False, zorder = 4)

    plt.plot(Tr, quant_LM, 's-', mfc = 'b', mec = 'k', mew = 1.2, color = 'b',
             lw = 1.5, label = 'L-momentos', clip_on = False, zorder = 4)
    plt.plot(Tr, inf_LM, '--', mfc = 'b', mec = 'k', mew = 1.2, color = 'b',
            lw = 1.5, label = 'L-momentos bandas de confianza', clip_on = False, zorder = 4)

    plt.legend(loc='best', numpoints = 1, fontsize = fontsize-1)

    plt.plot(Tr, sup_MEL, '--', mfc = 'r', mec = 'k', mew = 1.2, color = 'r',
            lw = 2.5, label = u'Máxima verosimilitud bandas de confianza', clip_on = False, zorder = 4)
    plt.plot(Tr, sup_LM, '--', mfc = 'b', mec = 'k', mew = 1.2, color = 'b',
            lw = 1.5, label = 'L-momentos bandas de confianza', clip_on = False, zorder = 4)

    #plt.ylim([0, 3500])
    plt.xlabel(u'Periodo de retorno [años]', fontsize = fontsize, labelpad = 0)
    plt.ylabel(label,
               fontsize = fontsize, labelpad = 0)
    # plt.ylabel('Nivel [m.s.n.m]', fontsize = fontsize, labelpad = 0)
    plt.tick_params(axis='x', which='both', labelsize=fontsize, direction = 'in')
    plt.tick_params(axis='y', which='both', labelsize=fontsize, direction = 'in')
    ax0.tick_params('both', length=7, width = 1.5, which='major')
    ax0.tick_params('both', length=4, width = 1.3, which='minor')
    plt.xscale('log')

    #plt.setp(ax0.get_xticklabels(), visible=False)
    for axis in ['top','bottom','left','right']:
         ax0.spines[axis].set_linewidth(1.8)

    plt.savefig(os.path.join(Path_out,'Cuantiles_' + Est + '.png'), dpi = 400)


    ####################   FIGURE
    plt.close('all')
    fig = plt.figure(figsize=(7, 5))
    gs = gridspec.GridSpec(1, 1)
    gs.update(left = 0.11, right = 0.96, top = 0.97, bottom = 0.13, hspace=0.12, wspace = 0.04)

    # Plot parameters
    fontsize = 15
    ymin = 0.0
    ymax = 0.5
    xmin = 0.0
    xmax = 0.5

    # Trpadec = np.array([2, 5, 10, 25, 50, 100, 500, 1000])
    # Qpadec = np.array([2465, 3280, 3820, 4508, 5018, 5524, 6695, 7198])

    ax0 = plt.subplot(gs[0])

    plt.plot(Tr, quant_LM, 's-', mfc = 'b', mec = 'k', mew = 1.2, color = 'b',
             lw = 1.5, label = 'L-momentos', clip_on = False, zorder = 4)
    plt.plot(Tr, inf_LM, '--', mfc = 'b', mec = 'k', mew = 1.2, color = 'b',
            lw = 1.5, label = 'L-momentos bandas de confianza', clip_on = False, zorder = 4)
    plt.plot(Tr, sup_LM, '--', mfc = 'b', mec = 'k', mew = 1.2, color = 'b',
            lw = 1.5, label = 'L-momentos bandas de confianza', clip_on = False, zorder = 4)

    plt.xlabel(u'Periodo de retorno [años]', fontsize = fontsize, labelpad = 0)
    plt.ylabel(label,
               fontsize = fontsize, labelpad = 0)
    # plt.ylabel('Nivel [m.s.n.m]', fontsize = fontsize, labelpad = 0)
    plt.tick_params(axis='x', which='both', labelsize=fontsize, direction = 'in')
    plt.tick_params(axis='y', which='both', labelsize=fontsize, direction = 'in')
    ax0.tick_params('both', length=7, width = 1.5, which='major')
    ax0.tick_params('both', length=4, width = 1.3, which='minor')
    plt.xscale('log')

    #plt.setp(ax0.get_xticklabels(), visible=False)
    for axis in ['top','bottom','left','right']:
         ax0.spines[axis].set_linewidth(1.8)
    # plt.legend(loc='best' numpoints = 1, fontsize = fontsize-1)
    plt.savefig(os.path.join(Path_out,'Cuantiles_LM_' + Est + '.png'), dpi = 400)


    cuantiles = pd.DataFrame( np.array([quant_LM,quant_MEL]).T, index = Tr, columns=['LM', 'MEL'])
    cuantiles.to_csv(os.path.join(Path_out,'Cuantiles_' + Est + '.csv'))

    quant = pd.Series(quant_LM,name=Est+'_LM', index=Tr)
    Resumen = Resumen.append(quant)
    quant = pd.Series(quant_MEL,name=Est+'_MEL', index=Tr)
    Resumen = Resumen.append(quant)

    SK_LM   = pd.Series(df.ks_MEL,name=Est+'_LM')
    SK_resm = SK_resm.append(SK_LM)
    SK_MEL  = pd.Series(df.ks_MEL,name=Est+'_MEL')
    SK_resm = SK_resm.append(SK_MEL)

    # intrval = pd.Series([inf, sup],name=Est, index=['Inferior', 'Superior'])
    # Intrval = Intrval.append(intrval)
    #
    bnd_MEL = pd.Series(np.append(inf_MEL, sup_MEL),name=Est+'_MEL', index= np.append(Tr, Tr))
    Bandas  = Bandas.append(bnd_MEL)
    bnd_LM  = pd.Series(np.append(inf_LM, sup_LM),name=Est+'_LM', index= np.append(Tr, Tr))
    Bandas  = Bandas.append(bnd_LM)
    # except:
    #     continue

if Est_path.endswith('CleanNiveles'):
    sufix = 'NR'
elif Est_path.endswith('CleanSedimentos'):
    sufix = 'Sed'
else:
    sufix = ''

Resumen.to_csv(os.path.join(Path_out,f'ResumenCuantiles_{sufix}.csv'))
SK_resm.to_csv(os.path.join(Path_out,f'ResumenSK_{sufix}.csv'))
# Intrval.to_csv(os.path.join(Path_out,f'Intervalos_{sufix}.csv'))
Bandas.to_csv(os.path.join(Path_out,f'Bandas_{sufix}.csv'))
