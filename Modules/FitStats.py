# -*- coding: utf-8 -*-
"""

@author: Andres AKA: peluca
Modified cmcuervol
"""

import pandas as pd
import numpy as np
import scipy.stats as st

############################   L-moments functions   ###########################

from Lmoments import Lmom, Lmom1
from Lmoments import L_ratiodiagram
from Lmoments import Lnorm, Lexp, Lgumbel, LGPA, LGEV, LGLO, LLOG3, LP3
from Lmoments import EXPcdf, GUMcdf, NORMcdf, GPAcdf, GEVcdf, GLOcdf, LLOG3cdf, LP3cdf
from Lmoments import EXPq, GUMq, NORMq, GPAq, GEVq, GLOq, LLOG3q, LP3q

dist_names = ['norm', 'expon', 'gumbel_r', 'genpareto',
              'genextreme', 'genlogistic', 'lognorm', 'pearson3']
names  = ['Norm.', 'Exp.', 'Gumbel', 'GPA', 'GEV',  'GLO',  'LOGN3', 'P3']
colors = ['k'   ,   'g' ,   'gold',   'b',   'r', 'lime', 'orange',  'm']

Ldist = [Lnorm, Lexp, Lgumbel, LGPA, LGEV, LGLO, LLOG3, LP3]
LCDF  = [NORMcdf, EXPcdf, GUMcdf, GPAcdf, GEVcdf, GLOcdf, LLOG3cdf, LP3cdf]
Lq    = [NORMq, EXPq, GUMq, GPAq, GEVq, GLOq, LLOG3q, LP3q]


################################   INPUT   #####################################

# Est = 'SanMarcos'
# data = pd.read_csv(Est + '.csv', index_col = 0, header = 0, low_memory=False)
# #dates = data.index
# #times = [plt.date2num(dt.datetime.strptime(d,'%Y-%m-%d')) for d in dates]
# #dates = [dt.datetime.strptime(d,'%Y-%m-%d') for d in dates]
# try:
#     serie = np.asarray(data.iloc[:,1])
# except:
#     serie = np.asarray(data.iloc[:,0])
# serie = serie[~np.isnan(serie)]*1.079


#################################   L-Moments   ################################
def LMfit(serie):
    """
    Fit serie with statistical distributions
    INPUTS
    Serie  : Array with the data (clean fo NaNs)
    OUTPUTS
    paramsLM  : fitting parameters
    ks_LM     : Kolmogorov Smirnov test
    pvalue_LM : fitting p-value
    """
    lmom, lmomA   = Lmom(serie)
    lmom1, t3, t4 = Lmom1(serie)

    paramsLM  = np.zeros((len(dist_names), 3))*np.NaN
    ks_LM     = np.zeros(len(dist_names))
    pvalue_LM = np.zeros(len(dist_names))

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
            Lfunc = Ldist[i]
            loc, scale, shape = Lfunc(lmom)
            cdfi = LCDF[i]
            cdf_fit = cdfi(x, loc, scale, shape)
            cdf = lambda x: cdfi(x, loc, scale, shape)
            ks_LM[i] = st.kstest(serie, cdf)[0]
            pvalue_LM[i] = st.kstest(serie, cdf)[1]
            paramsLM[i][:] = np.array([loc, scale, shape])

    return paramsLM, ks_LM, pvalue_LM

###########################   Maximum Likelihood   #############################

def MELfit(serie, returnLM=False):
    """
    Fit serie with statistical distributions
    INPUTS
    Serie  : Array with the data (clean fo NaNs)
    OUTPUTS
    paramsMEL  : fitting parameters
    ks_MEL     : Kolmogorov Smirnov test
    pvalue_MEL : fitting p-value
    """
    paramsMEL  = np.zeros((len(dist_names), 3))
    ks_MEL     = np.zeros(len(dist_names))
    pvalue_MEL = np.zeros(len(dist_names))

    paramsLM, ks_LM, pvalue_LM =  LMfit(serie)

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
        # cdf_fit = dist.cdf(x, *param[:-2], loc = param[-2], scale = param[-1])

        # K-S test
        ks_MEL[i] = st.kstest(serie, dist_name, param)[0]
        pvalue_MEL[i] = st.kstest(serie, dist_name, param)[1]
    if returnLM ==  True:
        return paramsLM, ks_LM, pvalue_LM, paramsMEL, ks_MEL, pvalue_MEL
    else:
        return paramsMEL, ks_MEL, pvalue_MEL


# df = pd.DataFrame({'ks_MEL': ks_MEL, 'ks_LM': ks_LM}, index = names)
# df.to_excel('Smirnov-Kolmogorov'+Est+'.xls')


################################################################################
##############################   Best distribution   ###########################
################################################################################
def BestFit(serie):
    """
    Found the best fit for a serie
    INPUTS
    Serie  : Array with the data (clean fo NaNs)
    OUTPUTS
    cdf_fitLM  : cdf of best LM fit
    cdf_fitMEL : cdf of best MEL fit
    x          : array x coordinate in the cdf
    dist_names : name of the distribution with the best fit
    """
    x = np.linspace(np.min(serie), np.max(serie), 100)
    paramsLM, ks_LM, pvalue_LM, paramsMEL, ks_MEL, pvalue_MEL = MELfit(serie,returnLM=True)

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

    return cdf_fitLM,cdf_fitMEL, x,  dist_names[index]

################################################################################
######################   Quantiles by best distribution   ######################
################################################################################

def QuantilBestFit(serie, Tr=None,):
    """
    Found the cuantiles for a determined return period in the best fit for a serie
    INPUTS
    Serie  : Array with the data (clean fo NaNs)
    Tr     : array with the return period
    OUTPUTS
    quant_LM  : cdf of best LM fit
    quant_MEL : cdf of best MEL fit
    dist_names: name of the distribution with the best fit
    """
    paramsLM, ks_LM, pvalue_LM, paramsMEL, ks_MEL, pvalue_MEL = MELfit(serie,returnLM=True)

    best_LM  = np.where(pvalue_LM==np.nanmax(pvalue_LM))[0][0]
    best_MEL = np.where(pvalue_LM==np.nanmax(pvalue_LM))[0][0]
    if best_LM == best_MEL:
        index = best_LM
    else:
        print(f"Choose between index {best_LM} and {best_MEL}")

    # twoparams = 1
    locMEL, scaleMEL, shapeMEL = paramsMEL[index,:]
    locLM,  scaleLM,  shapeLM  = paramsLM[index,:]
    distMEL = getattr(st, dist_names[index])
    distLM  = Lq[index]
    if Tr is None:
        Tr = np.array([2.33, 5, 10, 25, 50, 100, 200, 500, 1000])
    q = 1. - 1./Tr

    try:
        # Quantiles MEL
        quant_MEL = distMEL.ppf(q, loc = locMEL, scale = scaleMEL)
        # Quantiles LM
        quant_LM = distLM(q, locLM, scaleLM)
    except:
        # Quantiles MEL
        quant_MEL = distMEL.ppf(q, shapeMEL, loc = locMEL, scale = scaleMEL)
        # Quantiles LM
        quant_LM = distLM(q, locLM, scaleLM, shapeLM)

    return quant_LM,quant_MEL, dist_names[index]

# cuantiles = pd.DataFrame( np.array([quant_LM,quant_MEL]).T, index = Tr, columns=['LM', 'MEL'])
# cuantiles.to_csv('Cuantiles_' + Est + 'B.csv')
