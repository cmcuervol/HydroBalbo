# -*- coding: utf-8 -*-
"""
Created on Mon Oct 05 17:12:09 2020

@author: Andres
"""

import numpy as np
from scipy import stats as stats

################################################################################
################              PRUEBAS DE TENDENCIA             #################
################################################################################

def mk_test(x, alpha):
    """
    this perform the MK (Mann-Kendall) test to check if the trend is present in 
    data or not
    
    Input:
        x:   a vector of data
        alpha: significance level
    
    Output:
        h: True (if trend is present) or False (if trend is absence)
        p: p value of the significance test
        
    Examples
    --------
      >>> x = np.random.rand(100)
      >>> h,p = mk_test(x,0.05)  # meteo.dat comma delimited
    """
#    from __future__ import division
    from scipy.stats import norm
    n = len(x)
    
    # calculate S 
    s = 0
    for k in range(n-1):
        for j in range(k+1,n):
            s += np.sign(x[j] - x[k])
    
    # calculate the unique data
    unique_x = np.unique(x)
    g = len(unique_x)
    
    # calculate the var(s)
    if n == g: # there is no tie
        var_s = (n*(n-1)*(2*n+5))/18
    else: # there are some ties in data
        tp = np.zeros(unique_x.shape)
        for i in range(len(unique_x)):
            tp[i] = sum(unique_x[i] == x)
        var_s = (n*(n-1)*(2*n+5) + np.sum(tp*(tp-1)*(2*tp+5)))/18
    
    if s > 0:
        z = (s - 1)/np.sqrt(var_s)
    elif s == 0:
        z = 0
    elif s < 0:
        z = (s + 1)/np.sqrt(var_s)
    
    
    # calculate the p_value
    p = 2.*(1.-norm.cdf(abs(z))) # two tail test
    h = abs(z) > norm.ppf(1-alpha/2)
    
    return h, p

################################################################################

def T_trend(serie, alpha):
    
    # Prueba de tendencia T simple
    import scipy.stats as stats
    
    N = len(serie)
    t = np.arange(N)    
    
    rho = stats.stats.pearsonr(t, serie)[0]
    
    T = rho*np.sqrt((N-2)/(1.-rho**2))
    
    Tlim = stats.t.ppf(1.-alpha/2., N-2)
    
    h = T > Tlim
    
    return h
        

################################################################################
################            PRUEBAS CAMBIO VARIANZA            #################
################################################################################

def FSimple(serie1, serie2, alpha):
    
    s1 = np.std(serie1, ddof = 1)
    s2 = np.std(serie2, ddof = 1)

    
    # F Simple
    if s1 > s2:
        FS = (s1**2)/(s2**2)
        N1 = len(serie1)
        N2 = len(serie2)
    else:
        FS = (s2**2)/(s1**2)
        N1 = len(serie2)
        N2 = len(serie1)
    
    #Valores criticos F Simple-Test
    Fsup = stats.f.ppf(1.-alpha/2., N1-1, N2-1)
#    BS.append(banda_superior)
    Finf = stats.f.ppf(alpha/2., N1-1, N2-1)
#    BI.append(banda_inferior)
    
    #Prueba de cambio F Test
    if FS < Finf or FS > Fsup:
        h = True
#        fsimple.append('cambio')
    else:
        h = False
#        fsimple.append('no cambio')
    return h, FS
    

def FMod(serie1, serie2, alpha):
    
    #Parametros requeridos F Modified-Test
    n1 = len(serie1)
    n2 = len(serie2)
#    mu1 = np.mean(serie1)
#    mu2 = np.mean(serie2)
    s1 = np.std(serie1, ddof = 1)
    s2 = np.std(serie2, ddof = 1)
    
    rho1 = stats.pearsonr(serie1[:-1], serie1[1:])[0]
    rho2 = stats.pearsonr(serie2[:-1], serie2[1:])[0]
    
#    rho1 = np.sum((serie1[1:]-mu1)*(serie1[:-1]-mu1))/np.sum((serie1-mu1)**2)
#    rho2 = np.sum((serie2[1:]-mu2)*(serie2[:-1]-mu2))/np.sum((serie2-mu2)**2)
    
    num1 = (rho1**(n1+1.)) - (n1*rho1**2) + (n1-1.)*rho1
    num2 = (rho2**(n2+1.)) - (n2*rho2**2) + (n2-1.)*rho2
    
    NE1 = (n1**2)/(n1 + (2*num1)/((rho1-1)**2))
    NE2 = (n2**2)/(n2 + (2*num2)/((rho2-1)**2))
    
#    NE1 = (n1**2)/(n1+2*(((rho1**(n1+1))-(n1*rho1**2)+((n1-1)*rho1))/((rho1-1)**2)))
#    NE2 = (n2**2)/(n2+2*(((rho2**(n2+1))-(n2*rho2**2)+((n2-1)*rho2))/((rho2-1)**2)))
    
    sigma = ((NE1*(s1**2)) + (NE2*(s2**2)))/(NE1 + NE2)
    ss = 1. + ((1./NE1) + (1./NE2) - (1./(NE1 + NE2)))/3.
    
    # F Modificada
    FM = (((NE1+NE2)*np.log10(sigma))-(NE1*np.log10(s1**2))-(NE2*np.log10(s2**2)))/ss
    
    # Valor critico F Modified-Test
    lim_fm = stats.chi2.ppf(1.-alpha/2., 1)
    
    # Prueba de cambio F modified
    if FM > lim_fm:
        h = True
    else:
        h = False
    
    return h, FM


def Bartlett(serie1, serie2, alpha):
    
    B, p = stats.bartlett(serie1, serie2)
    
#    Prueba de cambio Bartlett test
    Bcrit = stats.chi2.ppf(1.-alpha, 2)
    if B > Bcrit:
        h = True
#        Bartlett.append('cambio')
    else:
        h = False
#        Bartlett.append('no cambio')
    
#    if p < alpha:
#        h = True
##        Bartlett.append('cambio')
#    else:
#        h = False
##        Bartlett.append('no cambio')
    
    return h

def AnsariBradley(serie1, serie2, alpha):
    
    AB, p = stats.ansari(serie1, serie2)
    
#    ABcrit = stats.norm.ppf(1.-alpha/2.)    
    
#    if AB < ABcrit:
#        h = True
##        Ansari.append('cambio')
#    else:
#        h = False
##        Ansari.append('no cambio')
    
    if p < alpha:
        h = True
#        Ansari.append('cambio')
    else:
        h = False
#        Ansari.append('no cambio')

    return h

def Levene(serie1, serie2, alpha):
    
    W, p = stats.levene(serie1, serie2)
    N = len(serie1) + len(serie2)
    k = 2
    
#    Fcrit_Levene = stats.f.ppf(0.95,9,90)
    Wcrit = stats.f.ppf(1.-alpha, k-1, N-k)
    if W > Wcrit:
        h = True
#        Ansari.append('cambio')
    else:
        h = False
#        Ansari.append('no cambio')    
    
#    if p < alpha:
#        h = True
##        Ansari.append('cambio')
#    else:
#        h = False
##        Ansari.append('no cambio')
    return h


################################################################################
#################            PRUEBAS CAMBIO MEDIA            ###################
################################################################################

def TSimple(serie1, serie2, alpha, var):

    n1 = len(serie1)
    n2 = len(serie2)
    
#    m1 = np.mean(serie1)
#    m2 = np.mean(serie2)
    s1 = np.std(serie1, ddof = 1)
    s2 = np.std(serie2, ddof = 1)
    
    var = np.logical_not(var)
    T, p = stats.ttest_ind(serie1, serie2, equal_var = var)
    
    # Misma varianza
    if var:
        Tcrit = stats.t.ppf(1.-alpha/2., n1+n2-2)
    else:
        Cnum = (((s1**2)/n1) + ((s2**2)/n2))**2
        Cden = (((s1**2)/n1)**2)/(n1-1) + (((s2**2)/n2)**2)/(n2-1)
        dof = np.floor(Cnum/Cden)
        Tcrit = stats.t.ppf(1.-alpha/2., dof)
    
#    Tman = (m1 - m2)*np.sqrt(n1*n2*(n1+n2-2.)/(n1+n2))/np.sqrt(n1*s1**2 + n2*s2**2)
    
#    Tsup = stats.t.ppf(1.-alpha/2., n1+n2-2)
#    Tinf = -stats.t.ppf(1.-alpha/2., n1+n2-2)
    
#    Tcrit = stats.t.ppf(1.-alpha, N1+N2-1)
#    # Prueba de cambio T simple
#    if T > Tsup or T < Tinf:
#        h = True
##        Tsimple.append('cambio')
#    else:
#        h = False
##        Tsimple.append('no cambio')

    if np.abs(T) > Tcrit:
        h = True
#        Tsimple.append('cambio')
    else:
        h = False
#        Tsimple.append('no cambio')

#    # Prueba de cambio T simple
#    if p < alpha:
#        h = True
##        Tsimple.append('cambio')
#    else:
#        h = False
##        Tsimple.append('no cambio')
    
    return h, T
    
def TMod(serie1, serie2, alpha, var):
    
    # Parametros requeridos T Modified-Test
    n1 = len(serie1)
    n2 = len(serie2)
    mu1 = np.mean(serie1)
    mu2 = np.mean(serie2)
    s1 = np.std(serie1, ddof = 1)
    s2 = np.std(serie2, ddof = 1)
    
    rho1 = stats.pearsonr(serie1[:-1], serie1[1:])[0]
    rho2 = stats.pearsonr(serie2[:-1], serie2[1:])[0]
    
#    rho1 = np.sum((serie1[1:]-mu1)*(serie1[:-1]-mu1))/np.sum((serie1-mu1)**2)
#    rho2 = np.sum((serie2[1:]-mu2)*(serie2[:-1]-mu2))/np.sum((serie2-mu2)**2)
    
    num1 = (rho1**(n1+1)) - (n1*rho1**2) + (n1-1)*rho1
    num2 = (rho2**(n2+1)) - (n2*rho2**2) + (n2-1)*rho2
    
    NE1 = (n1**2)/(n1 + (2*num1)/((rho1-1)**2))
    NE2 = (n2**2)/(n2 + (2*num2)/((rho2-1)**2))
    
#    NE1 = (n1**2)/(n1+2*(((rho1**(n1+1))-(n1*rho1**2)+((n1-1)*rho1))/((rho1-1)**2)))
#    NE2 = (n2**2)/(n2+2*(((rho2**(n2+1))-(n2*rho2**2)+((n2-1)*rho2))/((rho2-1)**2)))
    
    var = np.logical_not(var)
    # Si varianzas iguales
    if var:
        sigma = np.sqrt((np.sum((serie1 - mu1)**2) + np.sum((serie2 - mu2)**2))/(n1+n2-2))
        T = np.abs(mu1 - mu2)/(sigma*np.sqrt((1./NE1) + (1./NE2)))
        Tcrit = stats.t.ppf(1.-alpha/2., NE1+NE2-2)
    else:
        T = np.abs(mu1 - mu2)/(np.sqrt(((s1**2)/NE1) + ((s2**2)/NE2)))
        Cnum = (((s1**2)/n1) + ((s2**2)/n2))**2
        Cden = (((s1**2)/n1)**2)/(n1-1) + (((s2**2)/n2)**2)/(n2-1)
        dof = np.floor(Cnum/Cden)
        Tcrit = stats.t.ppf(1.-alpha/2., dof)
    
    #Prueba de cambio T modificada
    if T > Tcrit:
        h = True
#        Tmod.append('no cambio')
    else:
        h = False
#        Tmod.append('cambio')
        
#    sigma = ((NE1*(s1**2)) + (NE2*(s2**2)))/(NE1 + NE2)
#    ss = 1. + ((1./NE1) + (1./NE2) + (1./(NE1 + NE2)))/3.
#    T, p = stats.ttest_ind(serie1, serie2, equal_var = var)
#    
#    if var:
##        equal_mod = True
#        factor_t = np.sqrt(((1./n1)+(1./n2))/((1./NE1)+(1./NE2)))
#    else:
##        equal_mod = False
#        factor_t = np.sqrt((((s1**2)/n1)+((s2**2)/n2))/(((s1**2)/NE1)+((s2**2)/NE2)))
#    
#    TM = T*factor_t
#    
#    # Valor critico T Modificada
#    import math
#    gamma = (((s1**2/n1)+(s2**2/n2))**2)/((((s1**2/n1)**2)/(n1-1.))+(((s2**2/n2)**2)/(n2-1.)))
#    if var:
#        lim_tmod = stats.t.ppf(1.-alpha, NE1+NE2-2)
#    else:
##        equal_mod = False
#        lim_tmod = stats.t.ppf(1.-alpha, int(math.floor(gamma)))
#    
#    #Prueba de cambio T modificada
#    if TM < lim_tmod:
#        h = False
##        Tmod.append('no cambio')
#    else:
#        h = True
##        Tmod.append('cambio')
    
    return h, T
    
def UMann(serie1, serie2, alpha):

    U, p = stats.mannwhitneyu(serie1, serie2)
    
    Ucrit = stats.norm.ppf(1.-alpha/2.)
    
    # Prueba de cambio U - Mann Whitney
    if U > Ucrit:
        h = True
#        Mann.append('cambio')
    else:
        h = False
#        Mann.append('no cambio')    
    
    # Prueba de cambio U - Mann Whitney
    if 2*p < alpha:
        h = True
#        Mann.append('cambio')
    else:
        h = False
#        Mann.append('no cambio')

    return h

def KruskallWallis(serie1, serie2, alpha):

    KW, p = stats.mstats.kruskalwallis(serie1, serie2)
    
    #Prueba de cambio Kruskal Wallis
    if p < alpha:
        h = True
#        Kruskal.append('cambio')
    else:
        h = False
#        Kruskal.append('no cambio')
        
    return h


