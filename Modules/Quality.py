#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd
import datetime as dt

def QQadjust(Obs,Mod):
    """
    Mapping a quantile to quantile adjust
    INPUTS
    Obs : observation data
    Mod : Modeled Data
    OUTPUTS
    Cor : Modeled data adjust by quantiles to observation data
    """
    perc = np.arange(0,101)

    P_obs = np.percentile(Obs, perc)
    P_mod = np.percentile(Mod, perc)

    Cor = interp1d(perc, P_obs)(interp1d(P_mod, perc)(Mod))
    return Cor

def BiasParametric(Obs, Xref, X):
    """
    Parametric bias correction for modeled data based in the observations in a single point
    IMPUTS
    Obs  : array of observed Data
    Xref : array of modeled data that correspond to observed data, i.e. the same point
    X    : point of modeled data to correct, no necesarly in the same point of the observations
    OUTPUTS
    Xcor : array of modeled data with bias correction
    """
    # basic parameters
    O_m  = np.nanmean(Obs)
    Xr_m = np.nanmean(Xref)
    X_m  = np.nanmean(X)
    O_s  = np.nanstd(Obs)
    Xr_s = np.nanstd(Xref)
    X_s  = np.nanstd(X)

    N_m = X_m -(Xr_m-O_m) # new mean
    N_s = X_s*(O_s/Xr_s)  # new sta

    Xcor = N_m + (N_s/O_s)*(X-X_m) # bias correction of mean an scale

    return Xcor
