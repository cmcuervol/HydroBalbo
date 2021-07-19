#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import numpy as np
import pandas as pd
import datetime as dt

from Modules.Utils import Listador
from Modules import Read


Est_P = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Datos/CrudosP'))
Est_Q = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Datos/CrudosQ'))


if os.path.exists(os.path.join(os.path.dirname(__file__), 'CleanData/PPT')) == False:
    os.paht.makedirs(os.path.join(os.path.dirname(__file__), 'CleanData/PPT'))

if os.path.exists(os.path.join(os.path.dirname(__file__), 'CleanData/QDL')) == False:
    os.paht.makedirs(os.path.join(os.path.dirname(__file__), 'CleanData/QDL'))

Out_P = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData/PPT'))
Out_Q = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData/QDL'))

# Process the raw data
Files_P = Listador(Est_P, final='.csv')
Files_Q = Listador(Est_Q, final='.csv')

for i in range(len(Files_P)):
    Read.SplitIDEAMfile(filename=os.path.join(Est_P,Files_P[i]),Est_path=Out_P)

for i in range(len(Files_Q)):
    Read.SplitIDEAMfile(filename=os.path.join(Est_Q,Files_Q[i]),Est_path=Out_Q)

# Read.SplitAllIDEAM(Depatamento='NivelesAll', sept=';')
# Read.SplitAllIDEAM(Depatamento='CaudalesAll',sept=',')
