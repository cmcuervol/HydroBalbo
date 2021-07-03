# -*- coding: utf-8 -*-
"""
Created on Sun Oct 11 13:40:29 2020

@author: Andres
"""
import os
import numpy as np
from math import factorial as fact
from scipy.special import gamma
from scipy.stats import norm
from scipy.special import gammainc
import pylab as plt

################################   L-Momentos   ################################

def Lmom1(data):

    x = np.sort(data)
    n = len(x)

    # Calculation of Ponderated Weighted Moments (PWM) (Greenwood et al. 1979)
    ks = np.array([0, 1, 2, 3])
    Mk = np.zeros(len(ks))
    for j in range(len(ks)):
        k = ks[j]
        N = fact(n-1)/(fact(k)*fact(n-1-k))
        Mi = 0.
        for i in range(n-k):
            Ni = fact(n-(i+1))/(fact(k)*fact(n-(i+1)-k))
#            print Ni
            Mi = x[i]*Ni
            Mk[j] = Mk[j] + Mi
        Mk[j] = (1./n)*Mk[j]/N

    # Calculation of L-moments from PWMs (Hoskings & Wallis 1997)
    lmom = np.zeros(len(ks))
    lmom[0] = Mk[0]                                         # l1
    lmom[1] = 2.*Mk[1] - Mk[0]                              # l2
    lmom[2] = 6.*Mk[2] - 6.*Mk[1] + Mk[0]                   # l3
    lmom[3] = 20.*Mk[3] - 30.*Mk[2] + 12.*Mk[1] - Mk[0]     # l4

    t3 = lmom[2]/lmom[1]
    t4 = lmom[3]/lmom[1]

    return lmom, t3, t4

def Lmom(data):

    x = np.sort(data)
    n = len(x)

    # Calculation of Ponderated Weighted Moments (PWM) (Greenwood et al. 1979)
    rs = np.array([0, 1, 2, 3])
    br = np.zeros(len(rs))
    for i in range(len(rs)):
        r = rs[i]
        N = fact(n-1)/(fact(r)*fact(n-1-r))
        bi = 0.
        for j in range(r,n):
            Nj = fact(j+1-1)/(fact(r)*fact(j+1-1-r))
            bi = x[j]*Nj
            br[i] = br[i] + bi
        br[i] = (1./n)*br[i]/N

    b0 = np.mean(x)
    b1 = 0.
    for j in range(1,n):
        b1 = b1 + (j+1-1)*x[j]/(n-1)
    b1 = b1/n

    b2 = 0.
    for j in range(2,n):
        b2 = b2 + (j+1-1)*(j+1-2)*x[j]/((n-1)*(n-2))
    b2 = b2/n

    b3 = 0.
    for j in range(3,n):
        b3 = b3 + (j+1-1)*(j+1-2)*(j+1-3)*x[j]/((n-1)*(n-2)*(n-3))
    b3 = b3/n


    # Calculation of L-moments from PWMs (Hoskings & Wallis 1997)
    lmom = np.zeros(len(rs))
    lmom[0] = br[0]                                         # l1
    lmom[1] = 2.*br[1] - br[0]                              # l2
    lmom[2] = 6.*br[2] - 6.*br[1] + br[0]                   # l3
    lmom[3] = 20.*br[3] - 30.*br[2] + 12.*br[1] - br[0]     # l4

    lmomA = np.zeros(len(rs))
    lmomA[0] = b0                                           # l1
    lmomA[1] = 2.*b1 - b0                                   # l2
    lmomA[2] = 6.*b2 - 6.*b1 + b0                           # l3
    lmomA[3] = 20.*b3 - 30.*b2 + 12.*b1 - b0                # l4

    return lmom, lmomA

################################################################################
#########################   Two-parameter distributions   ######################
################################################################################

###########################   Exponential distribution   #######################

def Lexp(lmom):
    scale = 2.*lmom[1]
    loc = lmom[0] - scale

    return loc, scale

def EXPcdf(x, loc, scale):
    y = 1. - np.exp(-(x - loc)/scale)
    return y

def EXPq(x, loc, scale):
    q = loc - scale*np.log(1. - x)
    return q

##############################   Gumbel distribution   #########################

def Lgumbel(lmom):
    scale = lmom[1]/np.log(2.)
    loc = lmom[0] - scale*0.5772

    return loc, scale

def GUMcdf(x, loc, scale):
    y = np.exp(-np.exp(-(x - loc)/scale))
    return y

def GUMq(x, loc, scale):
    q = loc - scale*np.log(-np.log(x))
    return q

##############################   Normal distribution   #########################

def Lnorm(lmom):
    loc = lmom[0]
    scale = lmom[1]*(np.pi**0.5)

    return loc, scale

def NORMcdf(x, loc, scale):
    y = norm.cdf(x, loc = loc, scale = scale)
    return y

def NORMq(x, loc, scale):
    xp = np.linspace(loc, loc+20*scale, 1000)
    yp = NORMcdf(xp, loc, scale)
    q = np.interp(x, yp, xp)
    return q

################################################################################
#########################   Three-parameter distributions   ####################
################################################################################

#######################   Generalized Pareto distribution   ####################

def LGPA(lmom):

    t3 = lmom[2]/lmom[1]

    shape = (1.-3.*t3)/(1.+t3)
    scale = (1.+shape)*(2.+shape)*lmom[1]
    loc = lmom[0] - (2.+shape)*lmom[1]

    return loc, scale, shape

def GPAcdf(x, loc, scale, shape):
    y = -np.log(1. - shape*(x-loc)/scale)/shape
    F = 1 - np.exp(-y)
    # F[F<0] = 0.

    return F

def GPAq(x, loc, scale, shape):
    q = loc + scale*(1. - (1. - x)**shape)/shape

    return q

#######################   General Extreme Value distribution   #################

def LGEV(lmom):

    t3 = lmom[2]/lmom[1]

    c = (2./(3.+t3)) - (np.log(2.)/np.log(3.))
    shape = 7.8590*c + 2.9554*c**2
    scale = (lmom[1]*shape)/((1. - 2.**(-shape))*gamma(1.+shape))
    loc = lmom[0] - scale*(1. - gamma(1.+shape))/shape

    return loc, scale, shape

def GEVcdf(x, loc, scale, shape):
    y = -np.log(1. - shape*(x-loc)/scale)/shape
    F = np.exp(-np.exp(-y))

    return F

def GEVq(x, loc, scale, shape):

    q = loc + scale*(1. - (-np.log(x))**shape)/shape

    return q

#######################   Generalized Logistic distribution   ##################

def LGLO(lmom):

    t3 = lmom[2]/lmom[1]
    shape = -t3
    scale = (lmom[1]*np.sin(shape*np.pi))/(shape*np.pi)
    loc = lmom[0] - scale*((1./shape) - (np.pi/np.sin(shape*np.pi)))

    return loc, scale, shape

def GLOcdf(x, loc, scale, shape):
    y = -np.log(1. - shape*(x-loc)/scale)/shape
    F = 1./(1. + np.exp(-y))

    return F

def GLOq(x, loc, scale, shape):

    q = loc + scale*(1. - ((1. - x)/x)**shape)/shape

    return q

####################   Lognormal three-parameter distribution   ################

def LLOG3(lmom):

    t3 = lmom[2]/lmom[1]
    E = np.asarray([2.0466534, -3.6544371, 1.8396733, -0.20360244])
    F = np.asarray([1.0      , -2.0182173, 1.2420401, -0.21741801])
    t3poly = np.asarray([1., t3**2, t3**4, t3**6])
    shape = -t3*np.sum(E*t3poly)/np.sum(F*t3poly)

    scale = lmom[1]*shape*np.exp(-(shape**2)/2.)/(1. - 2.*norm.cdf(-shape/np.sqrt(2.)))

    loc = lmom[0] - (scale/shape)*(1. - np.exp((shape**2)/2.))

    return loc, scale, shape

def LLOG3cdf(x, loc, scale, shape):
    y = -np.log(1. - shape*(x-loc)/scale)/shape
    F = norm.cdf(y)

    return F

def LLOG3q(x, loc, scale, shape):

    xp = np.linspace(loc, loc+50*scale, 1000)
    yp = LLOG3cdf(xp, loc, scale, shape)
    q = np.interp(x, yp, xp)

    return q

########################   Pearson type III distribution   #####################

def LP3(lmom):

    t3 = lmom[2]/lmom[1]
    if 0 < np.abs(t3) < 1./3.:
        z = 3.*np.pi*t3**2
        a = (1. + 0.2906*z)/(z + 0.1882*z**2 + 0.0442*z**3)
    elif 1./3. <= np.abs(t3) < 1.:
        z = 1. - np.abs(t3)
        a = (0.36067*z-0.59567*z**2+0.25361*z**3)/(1.-2.78861*z+2.56096*z**2-0.77045*z**3)

    loc = lmom[0]
    scale = lmom[1]*(np.pi**0.5)*(a**0.5)*gamma(a)/gamma(a+0.5)
    shape = 2.*(a**(-0.5))*np.sign(t3)

    return loc, scale, shape

def LP3cdf(x, loc, scale, shape):
    if shape <= 0.:
        print ("Scale negative Pearson type III. Range not available")
        F = np.zeros(len(x))*np.NaN
    else:
        a = 4./(shape**2)
        b = 0.5*scale*shape
        c = loc - 2.*scale/shape

        F = gammainc(a, (x-c)/b)

    return F

def LP3q(x, loc, scale, shape):

    xp = np.linspace(loc, loc+50*scale, 1000)
    yp = LP3cdf(xp, loc, scale, shape)
    q = np.interp(x, yp, xp)

    return q

################################################################################
##############################   L-ratios diagram   ############################
################################################################################

def L_ratiodiagram(lmom, Est, PathFigs=None):

    t3 = lmom[2]/lmom[1]
    t4 = lmom[3]/lmom[1]

    # Two-parameter distributions
    names2p = ['Uniforme', 'Normal', 'Exponencial', 'Gumbel']
    Lskew2p = np.array([0, 0, 1./3., 0.1699])
    Lkurt2p = np.array([0, 0.1226, 1./6., 0.1504])
    marker = ['o', 's', 'v', 'd']
    color2p = ['k', 'k', 'k', 'k']

    # Three-parameter distributions
    names3p = ['GPA', 'GEV', 'GLO', 'LOGN3', 'P3']
    color3p = ['b', 'r', 'lime', 'orange', 'm']
    Lskew3p = np.arange(-1.,1.01,0.01)
    A0 = np.array([ 0.     ,  0.10701, 0.166667,  0.12282,  0.12240])
    A1 = np.array([ 0.20196,  0.11090,       0.,       0.,       0.])
    A2 = np.array([ 0.95924,  0.84838,  0.83333,  0.77518,  0.30115])
    A3 = np.array([-0.20096, -0.06669,       0.,       0.,       0.])
    A4 = np.array([ 0.04061,  0.00567,       0.,  0.12279,  0.95812])
    A5 = np.array([ 0.     , -0.04208,       0.,       0.,       0.])
    A6 = np.array([ 0.     ,  0.03763,       0., -0.13638, -0.57488])
    A7 = np.array([ 0.     ,       0.,       0.,       0.,       0.])
    A8 = np.array([ 0.     ,       0.,       0.,  0.11368,  0.19383])

    Lkurt3p = A0 + Lskew3p[:,None]*A1 + (Lskew3p[:,None]**2)*A2 + (Lskew3p[:,None]**3)*A3 + \
    (Lskew3p[:,None]**4)*A4 + (Lskew3p[:,None]**5)*A5 + (Lskew3p[:,None]**6)*A6 + \
    (Lskew3p[:,None]**7)*A7 + (Lskew3p[:,None]**8)*A8


    ################################   FIGURE   ####################################

    import matplotlib
#    import matplotlib.dates as mdates
    #import matplotlib.ticker as ticker
    matplotlib.rc('text', usetex = False)
    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    #matplotlib.rcParams['font.family'] = 'STIXGeneral'
    font = {'family':'serif', 'serif': ['Times New Roman']}
    plt.rc('font', **font)
    matplotlib.rcParams['text.latex.preamble']=[r'\usepackage{amsmath}']

    # Start figure and subfigures
    from matplotlib import gridspec
    fig = plt.figure(figsize=(7, 6))
    gs = gridspec.GridSpec(1, 1)
    gs.update(left = 0.10, right = 0.97, top = 0.98, bottom = 0.1, hspace=0.12, wspace = 0.04)

    # Plot parameters
    fontsize = 17
    ymin = 0.0
    ymax = 0.5
    xmin = 0.0
    xmax = 0.5
#    cmap = matplotlib.colors.ListedColormap(['grey','red'])

    ###############################   Time series   ################################

    ax0 = plt.subplot(gs[0])
    for sk, k, color, mark, dist in zip(Lskew2p, Lkurt2p, color2p, marker, names2p):
        plt.plot(sk, k, mark, ms = 8, mfc = color, mec = 'k', mew = 1., label = dist, zorder = 3,
                 clip_on = False)
    for i in range(len(names3p)):
        plt.plot(Lskew3p, Lkurt3p[:,i], color = color3p[i], lw = 2.5, zorder = 2,
                 label = names3p[i])
    plt.plot(t3, t4, 's', ms = 8, mfc = 'r', mec = 'k', zorder = 5, clip_on = False, label = Est)


    plt.ylim([ymin, ymax])
    plt.xlim([xmin, xmax])
    plt.xticks(np.arange(xmin, xmax+0.1, 0.1))
    plt.yticks(np.arange(ymin, ymax+0.1, 0.1))
    plt.ylabel('L-Kurtosis ($t_4$)', fontsize = fontsize, labelpad = 0)
    plt.xlabel('L-Skewness ($t_3$)', fontsize = fontsize, labelpad = 0)
    #ax0.xaxis.set_major_locator(mdates.DayLocator(bymonthday=(1), interval=1))
    plt.tick_params(axis='x', which='both', labelsize=fontsize, direction = 'in')
    plt.tick_params(axis='y', which='both', labelsize=fontsize, direction = 'in')
    ax0.tick_params('both', length=5, width = 1.5, which='major')
    #ax0.tick_params('both', length=3, width = 1.2, which='minor')
#    plt.setp(ax0.get_xticklabels(), visible=False)
    #plt.setp(ax0.get_yticklabels(), visible=False)
    for axis in ['top','bottom','left','right']:
         ax0.spines[axis].set_linewidth(1.5)
    ax0.legend(loc = 'center', bbox_to_anchor=(0.5, 0.8), ncol = 3, columnspacing = 0.5,
               handletextpad=0.1, numpoints = 1, handlelength=1.5, fontsize = fontsize-1,
               scatterpoints = 1, frameon = False)
    if PathFigs is not None:
        plt.savefig(os.path.join(PathFigs,Est+'.png'), dpi = 400)
    else:
        plt.savefig(Est+'.png', dpi = 400)
