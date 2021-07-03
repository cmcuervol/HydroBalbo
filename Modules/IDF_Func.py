# -*- coding: utf-8 -*-
"""
Created on Sun Oct 04 17:34:26 2020

@author: Andres
"""
import  os
import numpy as np
import scipy.stats as stats

def Wilches(data, Tr, theta1, theta2, Namefig=None, Path_Figs=None):

    # Moments of the annual 24-h maximum rainfall
    M1_24 = np.mean(data)
#    M2_24 = np.std(data)
    M2_24 = np.sqrt(np.sum((data - M1_24)**2)/len(data))
    CV_24 = M2_24/M1_24

    # Moments of the 105 min. duration rainfall
    M1_105 = M1_24*(105./1440.)**(theta1)
    #M2_105 = 1.27*M1_105**(1.947)
    CV_105 = CV_24               # Wilches (2001)

    ##############################   Return period   ###########################

    # Non-exceedence probability
    q = 1. - 1./Tr
    # Inverse cumulative normal distribution
    phiq = stats.norm.ppf(q)

    ################################   Duration   ##############################

    dlim1 = 1441
    dlim2 = 105
    dlim3 = 45
    dlim4 = 5
    # 105 < d < 1440
    d1 = np.arange(dlim2, dlim1, 5)
    # 45 < d < 105
    d2 = np.arange(dlim3, dlim2, 2.5)
    # 5 < d < 105
    d3 = np.arange(dlim4, dlim2, 2.5)
    # 5 < d < 45
    d4 = np.arange(dlim4, dlim3, 2.5)

    # IDF curves

    Idq1 = np.zeros((len(d1), len(Tr)))
    Idq2 = np.zeros((len(d2), len(Tr)))
    Idq3 = np.zeros((len(d3), len(Tr)))
    Idq4 = np.zeros((len(d4), len(Tr)))

    for i in range(len(Tr)):

        # 105 < d < 1440
        Idq1[:,i] = (M1_24*np.exp(phiq[i]*np.sqrt(np.log(1.+CV_24**2)))/np.sqrt(1.+CV_24**2))*(d1/1440.)**(theta1)

        # 45 < d < 105
        Idq2[:,i] = (M1_105*np.exp(phiq[i]*np.sqrt(np.log(1.+CV_105**2)))/np.sqrt(1.+CV_105**2))*(d2/105.)**(theta2)

        # 5 < d < 105
        Idq3[:,i] = Idq1[0,i]*((46.2/d3**0.75) - (43.05/d3))

        # 5 < d < 45
        Idq4[:,i] = Idq2[6,i]*((32.4/d4**0.75) - (30.00/d4))

    dA = np.hstack((d4, d2, d1))
    IdqA = np.vstack((Idq4, Idq2, Idq1))

    dB = np.hstack((d3, d1))
    IdqB = np.vstack((Idq3, Idq1))

#    np.savetxt('Idq1.txt', Idq1, fmt = '%.5f', delimiter = '    ')
#    np.savetxt('Idq2.txt', Idq2, fmt = '%.5f', delimiter = '    ')
    #np.savetxt('Idq3.txt', Idq3, fmt = '%.5f', delimiter = '    ')

    ################################   FIGURE   ################################

    if Namefig is not None:

        import matplotlib
        from matplotlib import gridspec
        matplotlib.rc('text', usetex = False)
        import matplotlib.pyplot as plt
        plt.rc('font',family='Times New Roman')

        # Start figure and subfigures
        fig = plt.figure(figsize=(9, 5))
        gs = gridspec.GridSpec(1, 1)
        gs.update(left = 0.07, right = 0.955, top = 0.965, bottom = 0.1, hspace=0.06, wspace = 0.04)

        fontsize = 15

        ax0 = plt.subplot(gs[0])
        for i in range(len(Tr)):
            plt.plot(dA, IdqA[:,i], color = 'b', lw = 2.2)
            plt.plot(dB, IdqB[:,i], color = 'r', ls = 'dashed', lw = 1.5)
        #    plt.plot(d, Idq[:,i], color = 'k', lw = 1.2)

        plt.xlim([0,250])
        plt.ylabel('Intensidad [mm/h]', fontsize = fontsize, labelpad = 0.)
        plt.xlabel(u'Duración [min]', fontsize = fontsize, labelpad = 0.)
        plt.tick_params(axis='x', which='both', labelsize=fontsize)
        plt.tick_params(axis='y', which='both', labelsize=fontsize)
        ax0.tick_params('both', length=5, width = 1.5, which='major')
        #plt.setp(ax0.get_xticklabels(), visible=False)
        #plt.setp(ax0.get_yticklabels(), visible=False)
        for axis in ['top','bottom','left','right']:
             ax0.spines[axis].set_linewidth(1.5)

        plt.plot(np.NaN, np.NaN, color = 'b', lw = 2.2, label = 'Modelo A')
        plt.plot(np.NaN, np.NaN, color = 'r', ls = 'dashed', lw = 1.5, label = 'Modelo B')

        ax0.legend(loc = 'center', bbox_to_anchor=(0.9, 0.5), ncol = 1, columnspacing=0.5,
                   handletextpad=0.1, numpoints = 1, handlelength=1.5, fontsize = fontsize-1,
                   scatterpoints = 1, frameon = False)

        plt.savefig(os.path.join(Path_Figs,Namefig+'IDF_Wilches.png'), dpi = 400)

    return dA, IdqA, IdqB



def Pulgarin(data, Tr, theta, Namefig=None, Path_Figs=None):

    ###################   Fit parameters Gumbel distribution   #################

#    # Method of Maximum likelihood (Python built-in)
#    gumbel_args = stats.gumbel_r.fit(data)
#    mud = gumbel_args[0]
#    alphad = gumbel_args[1]

    # Method of Ponderated Weighted Moments (Greenwood et al. 1979)
    from math import factorial as fact
    x = np.sort(data)
    n = len(x)
    ks = np.array([0, 1])
    Mk = np.zeros(len(ks))

    for j in range(len(ks)):
        k = ks[j]
        N = fact(n-1)/(fact(k)*fact(n-1-k))
        Mi = 0.
        for i in range(n-k):
            Ni = fact(n-(i+1))/(fact(k)*fact(n-(i+1)-k))
            # print (Ni)
            Mi = x[i]*Ni
            Mk[j] = Mk[j] + Mi
        Mk[j] = (1./n)*Mk[j]/N

    alphad = (Mk[0] - 2.*Mk[1])/np.log(2.)
    mud = Mk[0] - 0.5772*alphad

    y = -np.log(-np.log(1.-1./Tr))

    ################################   Duration   ##############################

    dlim1 = 1440
    dlim2 = 60
    dlim3 = 5
    d1 = np.arange(dlim2, dlim1, 5)
    d2 = np.arange(dlim3, dlim2, 2.5)

    # IDF curves
    Idq1 = np.zeros((len(d1), len(Tr)))
    Idq2 = np.zeros((len(d2), len(Tr)))

    for i in range(len(Tr)):
        Idq1[:,i] = (mud + alphad*y[i])*(d1/1440.)**theta
        Idq2[:,i] = Idq1[0,i]*((32.4/d2**0.75) - (30.00/d2))

    d = np.hstack((d2, d1))
    Idq = np.vstack((Idq2, Idq1))

    ################################   FIGURE   ################################

    if Namefig is not None:

        import matplotlib
        from matplotlib import gridspec
        matplotlib.rc('text', usetex = False)
        import matplotlib.pyplot as plt
        plt.rc('font',family='Times New Roman')

        # Start figure and subfigures
        fig = plt.figure(figsize=(9, 5))
        gs = gridspec.GridSpec(1, 1)
        gs.update(left = 0.07, right = 0.955, top = 0.965, bottom = 0.1, hspace=0.06, wspace = 0.04)

        fontsize = 15

        ax0 = plt.subplot(gs[0])
        for i in range(len(Tr)):
            plt.plot(d, Idq[:,i], color = 'b', lw = 2.2)
        #    plt.plot(d, Idq[:,i], color = 'k', lw = 1.2)

        plt.xlim([0,250])
        plt.ylabel('Intensidad [mm/h]', fontsize = fontsize, labelpad = 0.)
        plt.xlabel(u'Duración [min]',   fontsize = fontsize, labelpad = 0.)
        plt.tick_params(axis='x', which='both', labelsize=fontsize)
        plt.tick_params(axis='y', which='both', labelsize=fontsize)
        ax0.tick_params('both', length=5, width = 1.5, which='major')
        #plt.setp(ax0.get_xticklabels(), visible=False)
        #plt.setp(ax0.get_yticklabels(), visible=False)
        for axis in ['top','bottom','left','right']:
             ax0.spines[axis].set_linewidth(1.5)

        plt.savefig(os.path.join(Path_Figs,Namefig+'IDF_Pulgarin.png'), dpi = 400)

    return d, Idq

def VargasDiazGranados(data, Tr, Region, Namefig=None, Path_Figs=None):
    """
    Make IDF with Vargas Diaz Granados method 1998
    INPUTS
    data      : Anual max precipitation
    Tr        : return period
    Region    : integer to choose on of the followin regions
                Andina (R1)     0.94,0.18,0.66,0.83
                Caribe (R2)    24.85,0.22,0.5, 0.1
                Pacifico (R3)  13.92,0.19,0.58,0.2
                Orinoquia (R4)  5.53,0.17,0.63,0.42
    NameFig   : string to save figure
    Path_Figs : Absolute route to save figure
    """


    coef = np.array([[ 0.94,0.18,0.66,0.83],
                     [24.85,0.22,0.5, 0.1 ],
                     [13.92,0.19,0.58,0.2 ],
                     [ 5.53,0.17,0.63,0.42]])
    idR = Region -1
    dlim1 = 1440
    dlim2 = 60
    dlim3 = 5
    d1 = np.arange(dlim2, dlim1, 5)
    d2 = np.arange(dlim3, dlim2, 2.5)
    d = np.hstack((d2, d1))
    M = np.nanmean(data)

    Idq = np.zeros((len(d),len(Tr)),dtype=float)

    for i in range(len(Tr)):
        Idq[:,i] = coef[idR,0]*(Tr[i]**coef[idR,1])*(M**coef[idR,3])/((d/60)**coef[idR,2])

    ################################   FIGURE   ################################

    if Namefig is not None:

        import matplotlib
        from matplotlib import gridspec
        matplotlib.rc('text', usetex = False)
        import matplotlib.pyplot as plt
        plt.rc('font',family='Times New Roman')

        # Start figure and subfigures
        fig = plt.figure(figsize=(9, 5))
        gs = gridspec.GridSpec(1, 1)
        gs.update(left = 0.07, right = 0.955, top = 0.965, bottom = 0.1, hspace=0.06, wspace = 0.04)

        fontsize = 15

        ax0 = plt.subplot(gs[0])
        for i in range(len(Tr)):
            plt.plot(d, Idq[:,i], color = 'b', lw = 2.2)
        #    plt.plot(d, Idq[:,i], color = 'k', lw = 1.2)

        plt.xlim([0,250])
        plt.ylabel('Intensidad [mm/h]', fontsize = fontsize, labelpad = 0.)
        plt.xlabel(u'Duración [min]',   fontsize = fontsize, labelpad = 0.)
        plt.tick_params(axis='x', which='both', labelsize=fontsize)
        plt.tick_params(axis='y', which='both', labelsize=fontsize)
        ax0.tick_params('both', length=5, width = 1.5, which='major')
        #plt.setp(ax0.get_xticklabels(), visible=False)
        #plt.setp(ax0.get_yticklabels(), visible=False)
        for axis in ['top','bottom','left','right']:
             ax0.spines[axis].set_linewidth(1.5)

        plt.savefig(os.path.join(Path_Figs,Namefig+'IDF_VargasDiazGranados.png'), dpi = 400)

    return d, Idq

def IDEAM(Tr, X0, C1, C2, Namefig=None, Path_Figs=None):
    """
    Make IDF with IDEAM methodology
    INPUTS
    Tr        : return period
    X0        : Array of params
    C1        : Array of params
    C2        : Array of params
    NameFig   : string to save figure
    Path_Figs : Absolute route to save figure
    """
    dlim1 = 1440
    dlim2 = 60
    dlim3 = 5
    d1 = np.arange(dlim2, dlim1, 5)
    d2 = np.arange(dlim3, dlim2, 2.5)
    d = np.hstack((d2, d1))
    
    Idq = np.zeros((len(d),len(Tr)),dtype=float)

    for i in range(len(Tr)):
        Idq[:,i] = C1[i]/((d+X0[i])**C2[i])
    ################################   FIGURE   ################################

    if Namefig is not None:

        import matplotlib
        from matplotlib import gridspec
        matplotlib.rc('text', usetex = False)
        import matplotlib.pyplot as plt
        plt.rc('font',family='Times New Roman')

        # Start figure and subfigures
        fig = plt.figure(figsize=(9, 5))
        gs = gridspec.GridSpec(1, 1)
        gs.update(left = 0.07, right = 0.955, top = 0.965, bottom = 0.1, hspace=0.06, wspace = 0.04)

        fontsize = 15

        ax0 = plt.subplot(gs[0])
        for i in range(len(Tr)):
            plt.plot(d, Idq[:,i], color = 'b', lw = 2.2)
        #    plt.plot(d, Idq[:,i], color = 'k', lw = 1.2)

        plt.xlim([0,250])
        plt.ylabel('Intensidad [mm/h]', fontsize = fontsize, labelpad = 0.)
        plt.xlabel(u'Duración [min]',   fontsize = fontsize, labelpad = 0.)
        plt.tick_params(axis='x', which='both', labelsize=fontsize)
        plt.tick_params(axis='y', which='both', labelsize=fontsize)
        ax0.tick_params('both', length=5, width = 1.5, which='major')
        #plt.setp(ax0.get_xticklabels(), visible=False)
        #plt.setp(ax0.get_yticklabels(), visible=False)
        for axis in ['top','bottom','left','right']:
             ax0.spines[axis].set_linewidth(1.5)

        plt.savefig(os.path.join(Path_Figs,Namefig+'IDF_IDEAM.png'), dpi = 400)

    return d, Idq
