#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import numpy as np
import pandas as pd
import datetime as dt
from netCDF4 import Dataset
from tqdm import tqdm

Modules_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(Modules_dir)
from Utils import Listador, datetimer, WriteDict

Est_dir  = os.path.abspath(os.path.join(os.path.dirname(__file__), '../Datos/'))
Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../CleanData/'))

Sed_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../Datos/Sedimentos/'))
Sed_out = os.path.abspath(os.path.join(os.path.dirname(__file__), '../CleanSedimentos/'))

def SplitAllIDEAM(Depatamento=None, sept=',', Est_dir=Est_dir, Est_path=Est_path, Nivel=False):
    """
    Split Files downloaded with various estaciones in each file to create a file
    with data and another with metadata
    INPUTS
    sept        : strign of columns's separation
    Depatamento : Choose the Depatamento name to do the split,
                  by default is None, then all depataramentos will be splited
                  also can be a list or numpy array with the depataramentos
    Est_dir     : Path where the raw data are save
    Est_path    : Path where the procesed data will save

    """
    if Depatamento is None:
        Deps = os.listdir(Est_dir)
        if '.DS_Store' in Deps:
            Deps.remove('.DS_Store')
        if 'Icon\r' in Deps:
            Deps.remove('Icon\r')
    elif isinstance(Depatamento, (list,np.ndarray)):
        Deps = Depatamento
    else:
        Deps = [Depatamento]

    pbar = tqdm(total=len(Deps), desc='Spliting Departamento files: ')
    for Dep in Deps:
        Dep_dir = os.path.join(Est_dir, Dep)
        Files = Listador(Dep_dir,final='.csv')

        for archivo in Files:
            if Nivel == True:
                SplitIDEAMnivel(os.path.join(Dep_dir,archivo), sept, Est_path)
            else:
                SplitIDEAMfile(os.path.join(Dep_dir,archivo), sept, Est_path)
        pbar.update(1)
    pbar.close()

def SplitIDEAMfile(filename, sept=',', Est_path=Est_path):
    """
    Split an IDEAM file with some estaciones, to create a file of data an metadata for each estacion
    INPUTS
    filename : absolute path, name and extension of the file
    sept     : strign of columns's separation
    Est_path : Path where the procesed data will save
    """
    Dat = pd.read_csv(filename, sep=sept)
    print(filename)
    for Est_ID in np.unique(Dat.CodigoEstacion.values) :
        idx = np.where(Dat.CodigoEstacion.values==Est_ID)[0]
        meta = {'Parametro': 'Valor'}
        try:
            meta['NombreEstacion'  ] = np.unique(Dat.NombreEstacion  .iloc[idx])[0].replace(',','-')
            meta['Latitud'         ] = np.unique(Dat.Latitud         .iloc[idx])[0]
            meta['Longitud'        ] = np.unique(Dat.Longitud        .iloc[idx])[0]
            meta['Altitud'         ] = np.unique(Dat.Altitud         .iloc[idx])[0]
            meta['Categoria'       ] = np.unique(Dat.Categoria       .iloc[idx])[0].replace(',','-')
            meta['Entidad'         ] = np.unique(Dat.Entidad         .iloc[idx])[0].replace(',','-')
            meta['AreaOperativa'   ] = np.unique(Dat.AreaOperativa   .iloc[idx])[0].replace(',','-')
            meta['Departamento'    ] = np.unique(Dat.Departamento    .iloc[idx])[0].replace(',','-')
            meta['Municipio'       ] = np.unique(Dat.Municipio       .iloc[idx])[0].replace(',','-')
            meta['FechaInstalacion'] = np.unique(Dat.FechaInstalacion.iloc[idx])[0]
            meta['FechaSuspension' ] = np.unique(Dat.FechaSuspension .iloc[idx])[0]
            meta['IdParametro'     ] = np.unique(Dat.IdParametro     .iloc[idx])[0].replace(',','-')
            meta['Etiqueta'        ] = np.unique(Dat.Etiqueta        .iloc[idx])[0].replace(',','-')
            meta['DescripcionSerie'] = np.unique(Dat.DescripcionSerie.iloc[idx])[0].replace(',','-')
            meta['Frecuencia'      ] = np.unique(Dat.Frecuencia      .iloc[idx])[0].replace(',','-')
        except:
            meta['NombreEstacion'  ] = np.unique(Dat.NombreEstacion  .iloc[idx])[0]
            meta['Latitud'         ] = np.unique(Dat.Latitud         .iloc[idx])[0]
            meta['Longitud'        ] = np.unique(Dat.Longitud        .iloc[idx])[0]
            meta['Altitud'         ] = np.unique(Dat.Altitud         .iloc[idx])[0]
            meta['Categoria'       ] = np.unique(Dat.Categoria       .iloc[idx])[0]
            meta['Entidad'         ] = np.unique(Dat.Entidad         .iloc[idx])[0]
            meta['AreaOperativa'   ] = np.unique(Dat.AreaOperativa   .iloc[idx])[0]
            meta['Departamento'    ] = np.unique(Dat.Departamento    .iloc[idx])[0]
            meta['Municipio'       ] = np.unique(Dat.Municipio       .iloc[idx])[0]
            meta['FechaInstalacion'] = np.unique(Dat.FechaInstalacion.iloc[idx])[0]
            meta['FechaSuspension' ] = np.unique(Dat.FechaSuspension .iloc[idx])[0]
            meta['IdParametro'     ] = np.unique(Dat.IdParametro     .iloc[idx])[0]
            meta['Etiqueta'        ] = np.unique(Dat.Etiqueta        .iloc[idx])[0]
            meta['DescripcionSerie'] = np.unique(Dat.DescripcionSerie.iloc[idx])[0]
            meta['Frecuencia'      ] = np.unique(Dat.Frecuencia      .iloc[idx])[0]


        PPT = pd.DataFrame(Dat[['Valor', 'Grado','Calificador', 'NivelAprobacion']].iloc[idx].values,
                           index = pd.DatetimeIndex(Dat.Fecha.iloc[idx]),
                           columns = ['Valor', 'Grado','Calificador', 'NivelAprobacion'])

        PPT.to_csv(f'{os.path.join(Est_path,str(Est_ID))}.csv')
        WriteDict(meta, f'{os.path.join(Est_path,str(Est_ID))}.meta', sept=',')

def SplitIDEAMnivel(filename, sept=',', Est_path=Est_path):
    """
    Split an IDEAM file with some estaciones, to create a file of data an metadata for each estacion
    INPUTS
    filename : absolute path, name and extension of the file
    sept     : strign of columns's separation
    Est_path : Path where the procesed data will save
    """
    Dat = pd.read_csv(filename, sep=sept)
    print(filename)
    for Est_ID in np.unique(Dat.CodigoEstacion.values) :
        idx = np.where(Dat.CodigoEstacion.values==Est_ID)[0]
        meta = {'Parametro': 'Valor'}
        meta['NombreEstacion'  ] = np.unique(Dat.NombreEstacion  .iloc[idx])[0].replace(',','-')
        meta['Latitud'         ] = np.unique(Dat.Latitud         .iloc[idx])[0]
        meta['Longitud'        ] = np.unique(Dat.Longitud        .iloc[idx])[0]
        meta['Altitud'         ] = np.unique(Dat.Altitud         .iloc[idx])[0]
        meta['Categoria'       ] = np.unique(Dat.Categoria       .iloc[idx])[0].replace(',','-')
        meta['Entidad'         ] = np.unique(Dat.Entidad         .iloc[idx])[0].replace(',','-')
        meta['AreaOperativa'   ] = np.unique(Dat.AreaOperativa   .iloc[idx])[0].replace(',','-')
        meta['Departamento'    ] = np.unique(Dat.Departamento    .iloc[idx])[0].replace(',','-')
        meta['Municipio'       ] = np.unique(Dat.Municipio       .iloc[idx])[0].replace(',','-')
        meta['FechaInstalacion'] = np.unique(Dat.FechaInstalacion.iloc[idx])[0]
        meta['FechaSuspension' ] = np.unique(Dat.FechaSuspension .iloc[idx])[0]
        meta['IdParametro'     ] = np.unique(Dat.IdParametro     .iloc[idx])[0].replace(',','-')
        meta['Etiqueta'        ] = np.unique(Dat.Etiqueta        .iloc[idx])[0].replace(',','-')
        meta['DescripcionSerie'] = np.unique(Dat.DescripcionSerie.iloc[idx])[0].replace(',','-')
        meta['Frecuencia'      ] = np.unique(Dat.Frecuencia      .iloc[idx])[0].replace(',','-')

        PPT = pd.DataFrame(Dat[['Nivel real', 'Grado','Calificador', 'NivelAprobacion']].iloc[idx].values,
                           index = pd.DatetimeIndex(Dat.Fecha.iloc[idx]),
                           columns = ['Valor', 'Grado','Calificador', 'NivelAprobacion'])

        PPT.to_csv(f'{os.path.join(Est_path,str(Est_ID))}NR.csv')
        WriteDict(meta, f'{os.path.join(Est_path,str(Est_ID))}NR.meta', sept=',')


def EstacionCSV_np(name, col_name, path=Est_path):
    """
    Read flow or level data
    INPUTS
    name : station name
    path : folder where are save the data

    OUTPUTS
    D : DataFrame with the data, index are datetime
    """
    data  = np.genfromtxt(os.path.join(path,name),delimiter=',')
    index = np.genfromtxt(os.path.join(path,name),delimiter=',', dtype=str)
    D = pd.DataFrame(data[:,1].ravel(), index=pd.DatetimeIndex(index[:,0]), columns=[col_name])

    return D

def EstacionCSV_pd(name, col_name, path=Est_path):
    """
    Read flow or level data
    INPUTS
    name : station name
    path : folder where are save the data

    OUTPUTS
    D : DataFrame with the data, index are datetime
    """
    d = pd.read_csv(os.path.join(path,name),index_col=0)
    D = pd.DataFrame(d.Valor.values, index=pd.DatetimeIndex(d.index), columns=[col_name])

    return D

def FilesCFSv2(basedir, begin_date, end_date):
    """
    Make a list of netCDF files of CFSv2
    INPUTS:
    basedir    : directory root to search the files
    begin_date : datetime of the initial date, can include hour, but only 0,6,12,18
    end_date   : datetime of the final date, can include hour, but only 0,6,12,18
    OUTPUTS:
    files: list of total paths of the netCDF files
    """
    dates = datetimer(begin_date, end_date, 6)
    files = []
    for hour in dates:
        ruta1 = os.path.join(basedir, hour.strftime(r'%Y/%Y%m/%Y%m%d/'))
        ruta2 = os.path.join(basedir, hour.strftime(r'%Y/%Y%m/cfs.%Y%m%d/'))
        if os.path.exists(ruta1):
            ruta = ruta1
        elif os.path.exists(ruta2):
            ruta = ruta2
        else:
            print("No CFSv2 forecast found at {:%Y-%m-%d %H:%M}".format(hour))

        try:
            archivo = Listador(ruta, final=hour.strftime(r'%Y%m%d%H.nc'))
            # print(ruta, archivo)
            if len(archivo) == 0:
                print("No netCDF files found at {:%Y-%m-%d %H:%M}".format(hour))
            else:
                files.append(ruta+archivo[0])
        except:
            print("No CFSv2 forecast found at {:%Y-%m-%d %H:%M}".format(hour))

    return files


def Extract_NCvar(file, var=None, metavars=True):
    """
    Extract the variable especify an optionaly the arrays of latitude, longitude
    and time of CFSv2 netCDF files
    INPUTS:
    file : total path and name of the file
    var  : Name of the variable to stract. Default is None
    metavars: Bolean to extract arrays of latitude, longitude and time
    OUTPUTS:
    lat : array of latitudes
    lon : array of longitudes
    time: list of datetimes of the forecast dates
    """
    nc = Dataset(file,'r')
    if var is not None:
        Var = nc.variables[var][:,:,:]
    if metavars == True:
        t   = nc.variables['time'][:]
        lat = nc.variables['lat'][:]
        lon = nc.variables['lon'][:]

        zero = dt.datetime(2000,1,1)
        time = list(map(lambda x : zero+dt.timedelta(seconds=x*3600), t))
        # time  = [zero+dt.timedelta(seconds=x*3600) for x in t]
    nc.close()
    if var is not None:
        if metavars == True:
            return Var,lat, lon, time
        else:
            return Var
    elif metavars == True:
        return lat, lon, time
    else:
        return 'Asshole... chosse variable and/or metavars!'


def ReadIDEAM(file, name_col):
    """
    Read IDEAM precipitation file
    INPUTS
    file : string with the absolute route an name of the file to read
    name_col : Estación name to name the column in the DataFrame
    OUTPUTS
    DataFrame with the series of precipitation data in the IDEAM file
    """

    dat = pd.read_excel(file)

    data   = dat.iloc[:,2:].values
    years  = dat.iloc[:,0].values
    months = dat.iloc[:,1].values

    init = dt.datetime(years[0], months[0], 1)
    end_days = np.arange(31,27, -1)
    for d in end_days:
        try:
            end = dt.datetime(years[-1], months[-1], d)
            break
        except:
            pass

    dates = datetimer(init,end,24)
    dates = np.array(dates)
    Chorizo = np.zeros(len(dates), dtype=float)*np.nan
    print(name_col)
    for i in range(data.shape[0]):
        idt = np.where(dates == dt.datetime(years[i], months[i], 1))[0][0]
        if i < data.shape[0]-1:
            Chorizo[idt:idt+31] = data[i]
        else:
            for d in end_days:
                try:
                    Chorizo[idt:] = data[i,:d]
                    break
                except:
                    pass

    return pd.DataFrame(Chorizo, index=dates, columns=[name_col])

def ReadIDEAMall(path):
    """
    Read all IDEAM estaciones save in .xls files locate in the indicate path
    INPUTS
    path : absolute route where are locate the .xls files with IDEAM data

    OUPUT
    Estaciones : list with each DataFrame created with the IDEAM data of every estacion
    """

    files = Listador(path,final='.xls')

    Estaciones = []

    for name in files:
        esta = ReadIDEAM(os.path.join(path,name),name[:-4])
        Estaciones.append(esta)

    return Estaciones

def ReadMeta(Path=Est_path, extension='.meta'):
    """
    Read all Metadada
    INPUTS
    Path      : absolute path where the metadata are save
    extension : string of the metadata files end
    OUTPUTS
    Meta : DataFrame with the metadata of each estation
    """
    Files = Listador(Path,final=extension)
    pbar = tqdm(total=len(Files), desc='Reading metadata: ')
    for i, est in enumerate(Files):
        read = pd.read_csv(os.path.join(Path,est),header=0)
        if i == 0:
            cols = read.iloc[:,0].values
            vals = []
            indx = []
        vals.append(read.iloc[:,1].values)
        indx.append(est.replace(extension,''))
        pbar.update(1)
    pbar.close()

    Meta = pd.DataFrame(vals, index= indx, columns=cols)

    return Meta


def resolidos(Path_est, Path_out=Sed_out):
    """
    Read file resolidos
    """
    File = Listador(Path_est, inicio='resolidos', final='.csv')
    arch = os.path.join(Path_est, File[0])
    Dat = pd.read_csv(arch,index_col=0)
    Dat.index = pd.DatetimeIndex(Dat.index)

    Dat.to_csv(os.path.join(Path_out, File[0]), sep=',')

    return Dat

def MonthSediments(Path_est, Path_out=Sed_out):
    """
    Read File of monthly sedimets
    """
    File = Listador(Path_est, inicio='Valores_mensuales', final='.csv')
    arch = os.path.join(Path_est, File[0])
    Dat = pd.read_csv(arch,index_col=0)

    date = []
    Vals = Dat.values.reshape(Dat.shape[0]*Dat.shape[1])
    for i in range(Dat.shape[0]):
        for j in range(Dat.shape[1]):
            fecha = dt.datetime(Dat.index[i], j+1, 1)
            date.append(fecha)

    Datos = pd.DataFrame(Vals, index=date, columns=[' VALORES TOTALES MENSUALES DE TRANSPORTE (KTon/Dia)'])
    Datos.to_csv(os.path.join(Path_out, File[0]), sep=',')

    return Datos

def idinclude(year):
    """
    Make a list of indexes to exclude in a year of 31*12
    """
    c = 0
    inclu = []
    for m in range(12):
        for d in range(31):

            try:
                dt.datetime(year,m+1,d+1)
                inclu.append(c)
            except:
                pass
            c+=1
    return inclu


def  TransSediments(Path_est, Path_out=Sed_out):
    """
    Read File of monthly sedimets
    """
    File = Listador(Path_est, inicio='Trans', final='.csv')
    arch = os.path.join(Path_est, File[0])
    Dat = pd.read_csv(arch,index_col=0)

    Years = np.unique(Dat.index)

    for y in Years:
        init  = dt.datetime(y,1,1)
        end   = dt.datetime(y,12,31)
        dates = datetimer(init,end,24)
        dates = np.array(dates)

        include = idinclude(y)
        idy = np.where(Dat.index==y)[0]

        v = Dat.iloc[idy, 1:]
        V = v.values.reshape(31*12, order='F')
        V = V[include]

        df = pd.DataFrame(V, index=dates)
        if y == Years[0]:
            Vals = pd.DataFrame(V, index=dates)
        else:
            Vals = Vals.append(df)

    Vals.to_csv(os.path.join(Path_out, File[0]), sep=',')

    return Vals


def CleanSediments(Sed_dir=Sed_dir, Sed_out=Sed_out):
    """
    """
    Estaciones = Listador(Sed_dir)
    if '.DS_Store' in Estaciones:
        Estaciones.remove('.DS_Store')
    if 'Icon\r' in Estaciones:
        Estaciones.remove('Icon\r')

    gauge = Estaciones[0]
    Est = os.path.join(Sed_dir, gauge)

    for est in Estaciones:
        Est = os.path.join(Sed_dir, est)
        Campain = resolidos     (Path_est=Est, Path_out=Sed_out)
        Monthly = MonthSediments(Path_est=Est, Path_out=Sed_out)
        Diurnal = TransSediments(Path_est=Est, Path_out=Sed_out)
