#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import numpy as np
import pandas as pd

from windrose import WindroseAxes
from matplotlib import pyplot as plt
import matplotlib.cm as cm


Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Datos/Vientos'))


Magang = pd.read_excel(os.path.join(Est_path,'Magangue - Viento.xlsx'))
Marcos = pd.read_excel(os.path.join(Est_path,'San Marcos - Viento.xlsx'))


Magang_D = Magang['Dirección en °'].values
Magang_V = Magang['Vel (m/s)']     .values
Marcos_D = Marcos['Dirección en °'].values
Marcos_V = Marcos['Vel (m/s)']     .values


def WRbar(Dir, Vel, name='', title='', pdf=True, png=True, PathFigs=Est_path ):
    """
    Make wind rose in bars
    INPUTS
    Dir      : array with directions in north degrees
    Vel      : array with velocities
    name     : string to put as name in file
    title    : string to put as title
    pdf      : Boolean to save figure in pdf format
    png      : Boolean to save figure in png format
    PathFigs : Aboslute route to directory where figure will be save
    """

    plt.close('all')
    ax = WindroseAxes.from_ax()
    ax.bar(Dir, Vel, normed=True, opening=0.8, edgecolor='white')
    ax.set_legend(loc=4)
    ax.set_title(title)
    if pdf == True:
        plt.savefig(os.path.join(PathFigs, name+'BarWR.pdf'), format='pdf', transparent=True)
        if png == True:
            plt.savefig(os.path.join(PathFigs, name+'BarWR.png'), transparent=True)
    elif png == True:
        plt.savefig(os.path.join(PathFigs, name+'BarWR.png'), transparent=True)
    else:
        print("Graph not saved. To save it at least one of png or pdf parameters must be True.")

def WRcontourf(Dir, Vel, cmap='Spectral',name='', title='', pdf=True, png=True, PathFigs=Est_path ):
    """
    Make wind rose in bars
    INPUTS
    Dir      : array with directions in north degrees
    Vel      : array with velocities
    cmap     : cmap name to color map
    name     : string to put as name in file
    title    : string to put as title
    pdf      : Boolean to save figure in pdf format
    png      : Boolean to save figure in png format
    PathFigs : Aboslute route to directory where figure will be save
    """

    plt.close('all')
    ax = WindroseAxes.from_ax()
    ax.contourf(Dir, Vel, bins=np.arange(0, np.nanmax(Vel), 1),cmap=plt.get_cmap(cmap))
    ax.contour (Dir, Vel, bins=np.arange(0, np.nanmax(Vel), 1),colors='black')
    ax.set_legend(loc=4)
    ax.set_title(title)
    if pdf == True:
        plt.savefig(os.path.join(PathFigs, name+'contourfWR.pdf'), format='pdf', transparent=True)
        if png == True:
            plt.savefig(os.path.join(PathFigs, name+'contourfWR.png'), transparent=True)
    elif png == True:
        plt.savefig(os.path.join(PathFigs, name+'contourfWR.png'), transparent=True)
    else:
        print("Graph not saved. To save it at least one of png or pdf parameters must be True.")


WRbar(Magang_D,Magang_V,name='Magangue', title=u'Magangué', )
WRbar(Marcos_D,Marcos_V,name='Marcos', title=u'San Marcos', )

WRcontourf(Magang_D,Magang_V,name='Magangue', title=u'Magangué', )
WRcontourf(Marcos_D,Marcos_V,name='Marcos', title=u'San Marcos', )
