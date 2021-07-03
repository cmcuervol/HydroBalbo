# -*- coding: utf-8 -*-
# !/usr/bin/env python

import matplotlib
matplotlib.use ("Agg")
import os
import sys
import numpy as np
import pandas as pd
import datetime as dt

from matplotlib import cm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.colors as colors

from Utils import HistogramValues


# Colors for graphics SIATA style
gris70      = (112/255., 111/255., 111/255.)
ColorInfo1  = ( 82/255., 183/255., 196/255.)
ColorInfo2  = ( 55/255., 123/255., 148/255.)
ColorInfo3  = ( 43/255.,  72/255., 105/255.)
ColorInfo4  = ( 32/255.,  34/255.,  72/255.)
ColorInfo5  = ( 34/255.,  71/255.,  94/255.)
ColorInfo6  = ( 31/255., 115/255., 116/255.)
ColorInfo7  = ( 39/255., 165/255., 132/255.)
ColorInfo8  = (139/255., 187/255., 116/255.)
ColorInfo9  = (200/255., 209/255.,  93/255.)
ColorInfo10 = (249/255., 230/255.,  57/255.)


AzulChimba   = ( 55/255., 132/255., 251/255.)
AzulChimbita = ( 16/255., 108/255., 214/255.)
VerdeChimba  = (  9/255., 210/255.,  97/255.)
Azul         = ( 96/255., 200/255., 247/255.)
Naranja      = (240/255., 108/255.,  34/255.)
RojoChimba   = (240/255.,  84/255., 107/255.)
Verdecillo   = ( 40/255., 225/255., 200/255.)
Azulillo     = ( 55/255., 150/255., 220/255.)

ver = ( 77/255.,175/255., 74/255.)
azu = ( 55/255.,126/255.,184/255.)
roj = (228/255., 26/255., 28/255.)
nar = (252/255., 78/255., 42/255.)

rojo    = '#d7191c'
naranja = '#e66101'
verdeos = '#018571'
azul    = '#2c7bb6'
verde   = '#1a9641'
morado  = '#5e3c99'
magenta = '#dd3497'

Path = os.getcwd()

# =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
#                                    cmaps
# =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*


def cmapeador(colrs=None, levels=None, name='coqueto'):
    """
    Make a new color map with the colors and levels given, adapted from Julian Sepulveda & Carlos Hoyos

    IMPUTS
    colrs  : list of tuples of RGB colors combinations
    levels : numpy array of levels correspond to each color in colors
    name   : name to register the new cmap

    OUTPUTS
    cmap   : color map
    norm   : normalization with the levesl given optimized for the cmap
    """
    if colrs == None:
        colrs = [(255, 255, 255),(0, 255, 255), (0, 0, 255),(70, 220, 45),(44, 141, 29),\
                  (255,255,75),(255,142,0),(255,0,0),(128,0,128),(102,0,102),\
                  (255, 153, 255)]

    if levels == None:
        levels = np.array([0.,1.,5.,10.,20.,30.,45.,60., 80., 100., 150.])
    # print levels
    scale_factor   = ((255-0.)/(levels.max() - levels.min()))
    new_Limits     = list(np.array(np.round((levels-levels.min()) * scale_factor/255.,3),dtype=float))
    Custom_Color   = map(lambda x: tuple(ti/255. for ti in x) , colrs)
    nueva_tupla    = [((new_Limits[i]),Custom_Color[i],) for i in range(len(Custom_Color))]
    cmap_new       = colors.LinearSegmentedColormap.from_list(name,nueva_tupla)
    levels_nuevos  = np.linspace(np.min(levels),np.max(levels),255)
    # print levels_nuevos
    # print new_Limits
    # levels_nuevos  = np.linspace(np.min(levels),np.max(levels),1000)
    norm_new       = colors.BoundaryNorm(boundaries=levels_nuevos, ncolors=256)
    # norm           = colors.BoundaryNorm(boundaries=levels_nuevos, ncolors=1000)

    return cmap_new, norm_new


def newjet(cmap="jet"):
    """
    function to make a newd colorbar with white at center
    IMPUTS
    cmap: colormap to change
    RETURNS
    newcmap : colormap with white as center
    """
    jetcmap = plt.cm.get_cmap(cmap, 11) #generate a jet map with 11 values
    jet_vals = jetcmap(np.arange(11)) #extract those values as an array
    jet_vals[5] = [1, 1, 1, 1] #change the middle value
    newcmap = colors.LinearSegmentedColormap.from_list("newjet", jet_vals)
    return newcmap


class MidpointNormalize(colors.Normalize):
    """
    New Normalization with a new parameter: midpoint
    """
    def __init__(self, vmin=None, vmax=None, midpoint=None, clip=False):
        self.midpoint = midpoint
        colors.Normalize.__init__(self, vmin, vmax, clip)

    def __call__(self, value, clip=None):
        # I'm ignoring masked values and all kinds of edge cases to make a
        # simple example...
        x, y = [self.vmin, self.midpoint, self.vmax], [0, 0.5, 1]
        return np.ma.masked_array(np.interp(value, x, y))


class SqueezedNorm(matplotlib.colors.Normalize):
    def __init__(self, vmin=None, vmax=None, mid=0, s1=1, s2=1, clip=False):
        self.vmin = vmin # minimum value
        self.mid  = mid  # middle value
        self.vmax = vmax # maximum value
        self.s1=s1; self.s2=s2
        f = lambda x, zero,vmax,s: np.abs((x-zero)/(vmax-zero))**(1./s)*0.5
        self.g = lambda x, zero,vmin,vmax, s1,s2: f(x,zero,vmax,s1)*(x>=zero) - \
                                             f(x,zero,vmin,s2)*(x<zero)+0.5
        matplotlib.colors.Normalize.__init__(self, vmin, vmax, clip)

    def __call__(self, value, clip=None):
        r = self.g(value, self.mid,self.vmin,self.vmax, self.s1,self.s2)
        return np.ma.masked_array(r)


# fig, (ax, ax2, ax3) = plt.subplots(nrows=3,
#                                    gridspec_kw={"height_ratios":[3,2,1], "hspace":0.25})
#
# x = np.linspace(-13,4, 110)
# norm=SqueezedNorm(vmin=-13, vmax=4, mid=0, s1=1.7, s2=4)
#
# line, = ax.plot(x, norm(x))
# ax.margins(0)
# ax.set_ylim(0,1)
#
# im = ax2.imshow(np.atleast_2d(x).T, cmap="Spectral_r", norm=norm, aspect="auto")
# cbar = fig.colorbar(im ,cax=ax3,ax=ax2, orientation="horizontal")


def shiftedColorMap(cmap, start=0, midpoint=0.5, stop=1.0, min_val=None, max_val=None, name='shiftedcmap'):
    """
    Function to offset the "center" of a colormap. Useful for data with a \
    negative min and positive max and you want the middle of the colormap's dynamic \
    range to be at zero. Adapted from https://stackoverflow.com/questions/7404116/defining-the-midpoint-of-a-colormap-in-matplotlib

    IMPUTS
    -----
        cmap  : The matplotlib colormap to be altered.
        start : Offset from lowest point in the colormap's range.
                Defaults to 0.0 (no lower ofset). Should be between
                0.0 and `midpoint`.
        midpoint : The new center of the colormap. Defaults to
                   0.5 (no shift). Should be between 0.0 and 1.0. In
                   general, this should be  1 - vmax/(vmax + abs(vmin))
                   For example if your data range from -15.0 to +5.0 and
                   you want the center of the colormap at 0.0, `midpoint`
                   should be set to  1 - 5/(5 + 15)) or 0.75
        stop : Offset from highets point in the colormap's range.
               Defaults to 1.0 (no upper ofset). Should be between
               `midpoint` and 1.0.
        min_val : mimimun value of the dataset,
                  only use when 0.0 is pretend to be the midpoint of the colormap
        max_val : maximun value of the dataset,
                  only use when 0.0 is pretend to be the midpoint of the colormap
        name    : Name of the output cmap

    """
    epsilon = 0.001
    # start, stop = 0.0, 1.0
    if min_val is not None and max_val is not None:
        min_val, max_val = min(0.0, min_val), max(0.0, max_val)
        midpoint = 1.0 - max_val/(max_val + abs(min_val))

    cdict = {'red': [], 'green': [], 'blue': [], 'alpha': []}
    # regular index to compute the colors
    reg_index = np.linspace(start, stop, 257)
    # shifted index to match the data
    shift_index = np.hstack([np.linspace(0.0, midpoint, 128, endpoint=False), \
                            np.linspace(midpoint, 1.0, 129, endpoint=True)])
    for ri, si in zip(reg_index, shift_index):
        if abs(si - midpoint) < epsilon:
            r, g, b, a = cmap(0.5) # 0.5 = original midpoint.
        else:
            r, g, b, a = cmap(ri)
        cdict['red'].append((si, r, r))
        cdict['green'].append((si, g, g))
        cdict['blue'].append((si, b, b))
        cdict['alpha'].append((si, a, a))
    newcmap = matplotlib.colors.LinearSegmentedColormap(name, cdict)
    plt.register_cmap(cmap=newcmap)
    return newcmap


def GraphHydrografa(times, flow, title='', name='Hidrograma', pdf=True, png=False, PathFigs=Path,):
    """
    Grahp unitarian hydrograph
    INPUTS
    title    : Figure title
    name     : Name to save figure
    pdf      : Boolean to save figure in pdf format
    png      : Boolean to save figure in png format
    PathFigs : Aboslute route to directory where figure will be save
    """
    plt.close('all')
    figure =plt.figure(figsize=(12.8,8))

    ax = figure.add_subplot(1,1,1)
    ax.plot(times,flow, linewidth=2,color=azul)
    ax.set_xlabel('Time [hours]',fontsize=16)
    ax.set_ylabel(u'U [m$^{3}$ s$^{-1}$ mm$^{-1}$]',fontsize=16)
    ax.set_title(title,fontsize=16)
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


def GraphTc(Tim_dict,title='', name='ConcentrationTime', pdf=True, png=False, PathFigs=Path,):
    """
    Grahp of concentration times
    INPUTS
    Tim_dict : Dictionary with the concentration time, keys are the methodology name
    title    : Figure title
    name     : Name to save figure
    pdf      : Boolean to save figure in pdf format
    png      : Boolean to save figure in png format
    PathFigs : Aboslute route to directory where figure will be save
    """
    def autolabel(rects):
        """
        Attach a text label above each bar in *rects*, displaying its height.
        """
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{:.3f}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 1),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=8)

    vals = np.array(list(Tim_dict.values()))
    lims = np.percentile(vals, [25,75])
    idx = np.where((vals>lims[0])& (vals<lims[1]))[0]
    Tc_mean = np.mean(vals[idx])

    plt.close('all')
    figure =plt.figure(figsize=(12.8,8))

    ax = figure.add_subplot(1,1,1)
    rects = ax.bar(range(len(Tim_dict)), list(Tim_dict.values()), align='center', color=naranja)

    ax.fill_between(np.arange(-1,len(Tim_dict)+1), lims[0],lims[1],color=azul, alpha=0.8)
    ax.plot(np.arange(-1,len(Tim_dict)+1),[Tc_mean]*(len(Tim_dict)+2), '--', color=verde)

    ax.text(len(Tim_dict), Tc_mean,  '{:.3f}'.format(Tc_mean))
    ax.set_xlim(-1,len(Tim_dict)+1)
    ax.set_xticks(np.arange(len(Tim_dict.keys())))
    ax.set_xticklabels(list(Tim_dict.keys()))
    ax.set_ylabel(u'Concentration time [Hours]')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    # ax.spines['bottom'].set_visible(False)
    # ax.spines['left'].set_visible(False)
    autolabel(rects)

    if pdf == True:
        plt.savefig(os.path.join(PathFigs, name+'.pdf'), format='pdf', transparent=True)
        if png == True:
            plt.savefig(os.path.join(PathFigs, name+'.png'), transparent=True)
    elif png == True:
        plt.savefig(os.path.join(PathFigs, name+'.png'), transparent=True)
    else:
        print("Graph not saved. To save it at least one of png or pdf parameters must be True.")

def GraphIDF(Int, duration, frecuency, cmap_name='jet', name='IDF', pdf=True, png=False, PathFigs=Path,):
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
    fig = plt.figure(figsize=(12.8,8))
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

    ax.set_xlabel('Duration [minutes]',fontsize=16)
    ax.set_ylabel('Intensity [mm/hour]',fontsize=16)

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




def GraphHietogram(Pt, Pe,t, Tr,title='', name='Hietogram', pdf=True, png=False, PathFigs=Path,):
    """
    Make hietogram
    INPUTS
    Pt : Array with total precipitation [mm], shape(len(t),len(Tr))
    Pt : Array with loses precipitation [mm], shape(len(t),len(Tr))
    t  : list  or array with times [min]
    Tr : List  or array with return  times [years]
    """


    x = np.arange(len(t))  # the label locations
    width = 0.35  # the width of the bars

    columns = 3
    if (len(Tr)% columns) == 0:
        rows = len(Tr)/columns
    else:
        rows = len(Tr)/columns +1

    plt.close('all')
    # figure =plt.figure(figsize=(6.4*columns,4*rows))
    figure, axs = plt.subplots(figsize=(6.4*columns,4*rows),constrained_layout=True,
                               ncols=int(columns), nrows=int(rows))
    plt.title(title)

    # for  i in range(len(Tr)):
    for  i, ax in enumerate(axs.flat):
        # ax = figure.add_subplot(rows,columns,i+1)
        rects1 = ax.bar(x - width/2, Pt[:,i], width, label='Pt', color=azul)
        rects2 = ax.bar(x + width/2, Pe[:,i], width, label='Pe', color=rojo)

        # Add some text for labels, title and custom x-axis tick labels, etc.
        ax.set_ylabel('precipitation [mm]',fontsize=16)
        ax.set_xlabel('time [hours]',fontsize=16)
        ax.set_title(str(Tr[i]),fontsize=16)
        ax.set_xticks(x)
        ax.set_xticklabels(t.astype(str),rotation=90)
        ax.legend()

        def autolabel(rects):
            """
            Attach a text label above each bar in *rects*, displaying its height.
            """
            for rect in rects:
                height = rect.get_height()
                ax.annotate('{:.1f}'.format(height),
                            xy=(rect.get_x() + rect.get_width() / 2, height),
                            xytext=(0, 1),  # 3 points vertical offset
                            textcoords="offset points",
                            ha='center', va='bottom', fontsize=5)
        autolabel(rects1)
        autolabel(rects2)

        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        # ax.spines['bottom'].set_visible(False)
        # ax.spines['left'].set_visible(False)
    # figure.tight_layout()
    if pdf == True:
        plt.savefig(os.path.join(PathFigs, name+'.pdf'), format='pdf', transparent=True)
        if png == True:
            plt.savefig(os.path.join(PathFigs, name+'.png'), transparent=True)
    elif png == True:
        plt.savefig(os.path.join(PathFigs, name+'.png'), transparent=True)
    else:
        print("Graph not saved. To save it at least one of png or pdf parameters must be True.")


def GraphHydrogram(times, flow, Tr, join,
                   title='', name='Hidrograma', cmap_name='jet',
                   pdf=True, png=False, PathFigs=Path,):
    """
    Grahp unitarian hydrograph
    INPUTS
    times    : Array with times [hours]
    flow     : Array with flow   in diferent  return preiods
    Tr       : List  or array with return  times [years]
    join     : Boolean to  make in  a single figure
    title    : Figure title
    name     : Name to save figure
    cmap_name: color map name
    pdf      : Boolean to save figure in pdf format
    png      : Boolean to save figure in png format
    PathFigs : Aboslute route to directory where figure will be save
    """
    if join == True:
        # define some random data that emulates your indeded code:
        NCURVES = len(Tr)
        plt.close('all')
        fig = plt.figure(figsize=(12.8,8))
        ax = fig.add_subplot(111)
        cNorm  = colors.Normalize(vmin=0, vmax=NCURVES)
        scalarMap = cm.ScalarMappable(norm=cNorm, cmap=plt.get_cmap(cmap_name))

        lines = []
        for idx in range(NCURVES):
            line = flow[:, idx]
            colorVal  = scalarMap.to_rgba(idx)
            colorText = str(Tr[idx])+' years'
            retLine,  = ax.plot(times,line, linewidth=2,
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
        ax.set_xlabel('Time [hours]')
        ax.set_ylabel(u'Q [m$^{3}$ s$^{-1}$]')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        # ax.spines['bottom'].set_visible(False)
        # ax.spines['left'].set_visible(False)
    else:
        columns = 3
        if (len(Tr)% columns) == 0:
            rows = len(Tr)/columns
        else:
            rows = len(Tr)/columns +1

        plt.close('all')
        figure =plt.figure(figsize=(6.4*columns,4*rows))
        plt.title(title)
        for  i in range(len(Tr)):
            ax = figure.add_subplot(rows,columns,i+1)

            ax.plot(times,flow[:,i], linewidth=2,color=azul)
            ax.set_xlabel('Time [hours]',fontsize=16)
            ax.set_ylabel(u'Q [m$^{3}$ s$^{-1}$]',fontsize=16)
            ax.set_title(str(Tr[i]),fontsize=16)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            # ax.spines['bottom'].set_visible(False)
            # ax.spines['left'].set_visible(False)
        figure.tight_layout()
    if pdf == True:
        plt.savefig(os.path.join(PathFigs, name+'.pdf'), format='pdf', transparent=True)
        if png == True:
            plt.savefig(os.path.join(PathFigs, name+'.png'), transparent=True)
    elif png == True:
        plt.savefig(os.path.join(PathFigs, name+'.png'), transparent=True)
    else:
        print("Graph not saved. To save it at least one of png or pdf parameters must be True.")


def GraphQmax(Tr, Qmax_SCS, Qmax_Sny,Qmax_Wil, title='', name='Hidrograma', pdf=True, png=False, PathFigs=Path,):
    """
    Grahp unitarian hydrograph
    INPUTS
    Tr       : Return times [years]
    Qmax_SCS : Max flow of SCS
    Qmax_Sny : Max flow of Sneyder
    Qmax_Wil : Max flow of WilliansHann
    title    : Figure title
    name     : Name to save figure
    pdf      : Boolean to save figure in pdf format
    png      : Boolean to save figure in png format
    PathFigs : Aboslute route to directory where figure will be save
    """

    plt.close('all')
    figure =plt.figure(figsize=(12.8,8))

    ax = figure.add_subplot(1,1,1)
    ax.plot(Tr,Qmax_SCS, linewidth=2,color=morado, label='SCS')
    ax.plot(Tr,Qmax_Sny, linewidth=2,color=verde, label='Sneyder')
    ax.plot(Tr,Qmax_Wil, linewidth=2,color=azul, label='Willians & Hann')
    ax.set_xscale('log')
    ax.legend(loc=0)
    ax.set_xlabel('Return period [years]',fontsize=16)
    ax.set_ylabel(u'Flow [m$^{3}$ s$^{-1}$]',fontsize=16)
    ax.set_title(title,fontsize=16)
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

def GraphHistrogram(Values, bins=10, label='', title='', name='Histogram', pdf=True, png=False, PathFigs=Path,):
    """
    Grahp histogram
    INPUTS
    Values   : List or array to graph the histogram
    bins     : integer of the number of bins to calculate the histogram
    label    : string of the label
    title    : Figure title
    name     : Name to save figure
    pdf      : Boolean to save figure in pdf format
    png      : Boolean to save figure in png format
    PathFigs : Aboslute route to directory where figure will be save
    """
    histo = HistogramValues(Values, bins)
    plt.close('all')
    figure =plt.figure(figsize=(7.9,4.8))
    ax = figure.add_subplot(1,1,1)

    ax.plot(histo[1],histo[0],color=azul,lw=2)
    ax.fill_between(histo[1],histo[0],color=verde,alpha=0.6)
    ax.set_ylabel('Relative frecuency',fontsize=16)
    ax.set_xlabel(label,fontsize=16)

    # ax.plot(times,flow, linewidth=2,color=azul)
    # ax.set_xlabel('Time [hours]')
    # ax.set_ylabel(u'U [m$^{3}$ s$^{-1}$ mm$^{-1}$]')
    ax.set_title(title,fontsize=16)
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

def GraphEvents(Events, Unitarian=False, cmap_name='jet', name='Evento', pdf=True, png=False, PathFigs=Path,):
    """
    Graph of month diurnal cycles

    INPUTS
    Events    : List with lists or arrays of the events
    Unitarian : Boolean to graph with scale 0 to 1 time
    cmap_name : color map name
    name      : stringo for save the figure
    Path      : abtolute Path to save files
    """

    # define some random data that emulates your indeded code:
    NCURVES = len(Events)
    plt.close('all')
    fig = plt.figure(figsize=(12.8,8))
    ax = fig.add_subplot(111)
    cNorm  = colors.Normalize(vmin=0, vmax=NCURVES)
    scalarMap = cm.ScalarMappable(norm=cNorm, cmap=plt.get_cmap(cmap_name))

    lines = []
    for idx in range(NCURVES):
        # line = Events[idx]
        colorVal  = scalarMap.to_rgba(idx)
        colorText = f'Evento {idx+1}'

        if Unitarian == False:
            x_vals = np.arange(len(Events[idx]))
        else:
            x_vals = np.arange(len(Events[idx]))/(len(Events[idx])-1)

        retLine,  = ax.plot(x_vals,Events[idx],
                            linewidth=1,
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
    if Unitarian == False:
        ax.set_xlabel('Duration [days]')
    else:
        ax.set_xlabel('Duration fraction',)
    ax.set_ylabel('Q [m$^{3}$ s$^{-1}$]')

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

def GraphSerieOutliers(Serie, lim_inf, lim_sup, label='', title='', name='Outliers1', pdf=True, png=False, PathFigs=Path,):
    """
    Grahp serie with ouliers
    INPUTS
    Serie    : DataFrame array with data
    lim_inf  : float of bottom threshold to consider outliers
    lim_sup  : float of upper threshold to consider outliers
    label    : string of the label
    title    : Figure title
    name     : Name to save figure
    pdf      : Boolean to save figure in pdf format
    png      : Boolean to save figure in png format
    PathFigs : Aboslute route to directory where figure will be save
    """
    plt.close('all')
    figure =plt.figure(figsize=(7.9,4.8))
    ax = figure.add_subplot(1,1,1)

    ax.plot(np.arange(len(Serie)),Serie.values,color=azul,lw=2)
    ax.fill_between(np.arange(len(Serie)),[lim_sup]*len(Serie),[lim_inf]*len(Serie),color=verde,alpha=0.6)
    ax.set_ylabel(label, fontsize=16)
    # ticks = ax.get_xticks()
    # labels = [Serie.index[int(i)].strftime('%H:%M') for i in ticks[:-1]]
    start, end = ax.get_xlim()

    ax.xaxis.set_ticks(np.linspace(start, end, 10))
    ticks = ax.get_xticks()
    mes = ['Ene','Feb','Mar', 'Abr', 'May','Jun','Jul',
           'Ago', 'Sep','Oct','Nov','Dic']
    labels = [mes[Serie.index[int(i)][1]-1]+'\n'+str(Serie.index[int(i)][0]) for i in ticks[1:-1]]
    labels.insert(0,'')
    labels.append('')
    # ax.set_xticklabels(labels,fontsize=16)
    # start, end = ax.get_xlim()
    # ax.xaxis.set_ticks(np.linspace(start, end, 8))
    # ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
    # ax.set_xlabel(label)

    ax.set_title(title,fontsize=16)
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


def GraphSerieOutliersMAD(Serie, Outliers, label='', title='', name='Outliers1', pdf=True, png=False, PathFigs=Path,):
    """
    Grahp serie with ouliers
    INPUTS
    Serie    : DataFrame array with data
    lim_inf  : float of bottom threshold to consider outliers
    lim_sup  : float of upper threshold to consider outliers
    label    : string of the label
    title    : Figure title
    name     : Name to save figure
    pdf      : Boolean to save figure in pdf format
    png      : Boolean to save figure in png format
    PathFigs : Aboslute route to directory where figure will be save
    """
    plt.close('all')
    figure =plt.figure(figsize=(7.9,4.8))
    ax = figure.add_subplot(1,1,1)

    ax.plot(Serie,color=azul,lw=2, label='Serie')
    ax.plot(Outliers,marker='*',color=rojo, linestyle='None', label='Outliers')
    ax.set_ylabel(label,fontsize=16)
    # ticks = ax.get_xticks()
    # labels = [Serie.index[int(i)].strftime('%H:%M') for i in ticks[:-1]]
    start, end = ax.get_xlim()
    ax.xaxis.set_ticks(np.linspace(start, end, 8))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
    # ax.set_xlabel(label,fontsize=16)
    ax.legend(loc=0)
    ax.set_title(title,fontsize=16)
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

def DurationCurve(T_super, Values, label='', title='', name='DurationCurve', pdf=True, png=False, PathFigs=Path,):
    """
    Grahp serie with ouliers
    INPUTS
    T_super  : Array of percentaje of superation
    Values   : Array with data
    label    : string of the label
    title    : Figure title
    name     : Name to save figure
    pdf      : Boolean to save figure in pdf format
    png      : Boolean to save figure in png format
    PathFigs : Aboslute route to directory where figure will be save
    """
    plt.close('all')
    figure =plt.figure(figsize=(7.9,4.8))
    ax = figure.add_subplot(1,1,1)

    ax.scatter(T_super,Values, s=10, c=rojo)
    ax.set_ylabel(label,fontsize=16)
    ax.set_xlabel('Porcentaje de excedencia',fontsize=16)

    ax.set_title(title,fontsize=16)
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




def GraphDataFrames(DFs, Names, col, label=None, cmap_name='nipy_spectral',
                    name='Sediments', pdf=True, png=False, PathFigs=Path,):
    """
    Graph several DataFrames
    INPUTS
    DFs       : List of DataFrames
    Names     : List with names of each DataFrame
    col       : integer of column number to graph
    label     : String to put as y-label, default is the name of column choosen
    cmap_name : color map name
    name      : stringo for save the figure
    Path      : abtolute Path to save files
    """

    # define some random data that emulates your indeded code:
    NCURVES = len(DFs)
    plt.close('all')
    fig = plt.figure(figsize=(12.8,8))
    ax = fig.add_subplot(111)
    cNorm  = colors.Normalize(vmin=0, vmax=NCURVES)
    scalarMap = cm.ScalarMappable(norm=cNorm, cmap=plt.get_cmap(cmap_name))

    lines = []
    for idx in range(NCURVES):
        colorVal  = scalarMap.to_rgba(idx)
        colorText = Names[idx]
        retLine,  = ax.plot(DFs[idx].iloc[:,col],
                            linewidth=2,
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

    # ax.set_xlabel('Duration [minutes]',)
    if label is None:
        ax.set_ylabel(DFs[0].columns[col])
    else:
        ax.set_ylabel(label)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))

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


def GraphSingleDF(DF, label=None, title='', color=azul,
                  name='Sediments', pdf=True, png=False, PathFigs=Path,):
    """
    Graph single DataFrames
    INPUTS
    DFs       : DataFrames
    label     : String to put as y-label, default is the name of column choosen
    name      : stringo for save the figure
    Path      : abtolute Path to save files
    """

    # define some random data that emulates your indeded code:
    plt.close('all')
    fig = plt.figure(figsize=(7.9,4.8))
    ax = fig.add_subplot(111)

    ax.plot(DF,linewidth=2,color=color)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))

    if label is None:
        ax.set_ylabel(DF[0].columns[col],fontsize=16)
    else:
        ax.set_ylabel(label,fontsize=16)
    ax.set_title(title,fontsize=16)

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

def GraphCorrelogram(Corr, title='', color=rojo,
                     name='Correlogram', pdf=True, png=False, PathFigs=Path,):
    """
    Graph correlogram
    INPUTS
    Corr      : Array with correlation
    label     : String to put as y-label, default is the name of column choosen
    name      : stringo for save the figure
    Path      : abtolute Path to save files
    """

    # define some random data that emulates your indeded code:
    plt.close('all')
    fig = plt.figure(figsize=(7.9,4.8))
    ax = fig.add_subplot(111)

    ax.plot(Corr,linewidth=2,color=color)

    ax.set_xlabel('Rezago',fontsize=16)
    ax.set_ylabel(u'Correlación',fontsize=16)
    ax.set_title(title,fontsize=16)

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

def GraphSerieENSO(INDEX, serie, twin=True, labelENSO='', labelSerie='', title='',
                     name='ENSO', pdf=True, png=False, PathFigs=Path,):
    """
    Graph correlogram
    INPUTS
    Corr      : Array with correlation
    label     : String to put as y-label, default is the name of column choosen
    name      : stringo for save the figure
    Path      : abtolute Path to save files
    """

    # define some random data that emulates your indeded code:
    plt.close('all')
    fig = plt.figure(figsize=(7.9,4.8))
    ax = fig.add_subplot(111)
    if twin == True:
        ax.plot(INDEX,linewidth=2,color=rojo)

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
        ax.set_ylabel(labelENSO, color=rojo,fontsize=16)
        ax.tick_params(axis='y', labelcolor=rojo)

        ax2 = ax.twinx()
        ax2.plot(serie,linewidth=2,color=verde)
        ax2.set_ylabel(labelSerie, color=verde,fontsize=16)
        ax2.tick_params(axis='y', labelcolor=verde)
    else:
        ax.plot(serie,linewidth=2,label=labelSerie,color=verde)
        ax.plot(INDEX,linewidth=2,label=labelENSO, color=rojo)

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
        ax.set_ylabel('Anomalias',fontsize=16)
        ax.legend(loc=0)


    ax.set_title(title,fontsize=16)
    ax.spines['top'].set_visible(False)
    # ax.spines['right'].set_visible(False)
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

def GraphScatter(X,Y,xlabel='', ylabel='', title='', name='Scatter', pdf=True, png=False, PathFigs=Path,):
    """
    Grahp histogram
    INPUTS
    Values   : List or array to graph the histogram
    bins     : integer of the number of bins to calculate the histogram
    label    : string of the label
    title    : Figure title
    name     : Name to save figure
    pdf      : Boolean to save figure in pdf format
    png      : Boolean to save figure in png format
    PathFigs : Aboslute route to directory where figure will be save
    """
    reg = np.poly1d(np.polyfit(X, Y, 1))
    xp  = np.arange(X.min(), X.max(), 100)
    plt.close('all')
    figure =plt.figure(figsize=(7.9,4.8))
    ax = figure.add_subplot(1,1,1)

    ax.scatter(X,Y,c=azul)
    ax.plot(xp,reg(xp),'--',color=verde, linewidth=1)
    ax.set_ylabel(ylabel,fontsize=16)
    ax.set_xlabel(xlabel,fontsize=16)

    # ax.plot(times,flow, linewidth=2,color=azul)
    # ax.set_xlabel('Time [hours]')
    # ax.set_ylabel(u'U [m$^{3}$ s$^{-1}$ mm$^{-1}$]')
    ax.set_title(title,fontsize=16)
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
