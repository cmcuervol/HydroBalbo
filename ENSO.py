#-*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import datetime as dt
import os
import io
import requests
from dateutil.relativedelta import relativedelta

from Modules import Read
from Modules import Graphs
from Modules.Utils import Listador, FindOutlier, Cycles


def SSTregions():
    """
    Read SST weekly anomalies and put them in a DataFrame
    OUPUTS:
    SST : DataFrame with the anomalies of SST in El Niño regions
    """

    SSTweek = 'https://www.cpc.ncep.noaa.gov/data/indices/wksst8110.for'

    s  = requests.get(SSTweek).content

    date  = []
    N12   = []
    N12_A = []
    N3    = []
    N3_A  = []
    N34   = []
    N34_A = []
    N4    = []
    N4_A  = []

    with io.StringIO(s.decode('utf-8')) as f:
        data = f.readlines()
    for d in data[4:]:
        d = d.strip()
        d = d.split('     ')
        date.append(dt.datetime.strptime(d[0], '%d%b%Y'))
        N12  .append(float(d[1][:4]))
        N12_A.append(float(d[1][4:]))
        N3   .append(float(d[2][:4]))
        N3_A .append(float(d[2][4:]))
        N34  .append(float(d[3][:4]))
        N34_A.append(float(d[3][4:]))
        N4   .append(float(d[4][:4]))
        N4_A .append(float(d[4][4:]))

    SST = pd.DataFrame(np.array([N12_A,N3_A,N34_A,N4_A]).T, index=date, \
                       columns=[u'Niño1+2',u'Niño3',u'Niño34',u'Niño4'])
    return SST

def ONIdata():
    """
    Read ONI data and put them in a DataFrame
    OUPUTS:
    ONI : DataFrame with the ONI data
    """
    linkONI = 'https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt'
    s = requests.get(linkONI).content

    Season = []
    year   = []
    Total  = []
    Anom   = []
    date   = []

    with io.StringIO(s.decode('utf-8')) as f:
        data = f.readlines()
    m = 0
    for d in data[1:]:
        d = d.strip()
        d = d.split()
        Season.append(d[0])
        year  .append(int(d[1]))
        Total .append(float(d[2]))
        Anom  .append(float(d[3]))
        date  .append(dt.datetime(1950,2,1)+relativedelta(months=m))
        m+=1
    ONI = pd.DataFrame(np.array([Anom, Total, Season]).T, index=date, \
                       columns=[u'Anomalie', u'Total',u'Season'])

    return ONI

def SOIdata():
    """
    Read ONI data and put them in a DataFrame
    OUPUTS:
    ONI : DataFrame with the ONI data
    """
    # linkSOI = 'http://www.bom.gov.au/climate/enso/soi.txt'
    linkSOI = 'https://www.ncdc.noaa.gov/teleconnections/enso/indicators/soi/data.csv'
    s = requests.get(linkSOI).content

    date = []
    soi  = []

    with io.StringIO(s.decode('utf-8')) as f:
        data = f.readlines()
    m = 0
    for i in range(len(data)):
        if i >=2:
            row = data[i].strip()
            val = row.split(',')
            date.append(dt.datetime.strptime(val[0], '%Y%m'))
            soi.append(float(val[1]))
    SOI = pd.DataFrame(np.array(soi).T, index=date, columns=[u'SOI'])

    return SOI

def MEIdata():
    """
    Read ONI data and put them in a DataFrame
    OUPUTS:
    ONI : DataFrame with the ONI data
    """
    linkMEI = 'https://psl.noaa.gov/enso/mei/data/meiv2.data'
    s = requests.get(linkMEI).content

    date = []
    mei  = []

    with io.StringIO(s.decode('utf-8')) as f:
        data = f.readlines()
    lims = np.array(data[0].strip().split('   ')).astype(int)
    for i in range(len(data)):
        if i >=1:
            row = data[i].strip()
            val = row.split('    ')
            for m in range(12):
                date.append(dt.datetime(int(val[0]),m+1,1))
            mei.append(np.array(val[1:]).astype(float))
            if int(val[0])== lims[1]-1:
                break
    mei = np.array(mei).reshape(len(mei)*12)
    MEI = pd.DataFrame(np.array(mei).astype(float), index=date, columns=[u'MEI'])

    return MEI

# Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData'))
# Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanNiveles'))
# Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanSedimentos'))
Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Datos/ENSO_P'))
Path_out = os.path.abspath(os.path.join(os.path.dirname(__file__), 'ENSO'))


SOI = SOIdata()
MEI = MEIdata()
ONI = ONIdata()
ONI = ONI['Anomalie'].astype(float)
SST = SSTregions()



if Est_path.endswith('CleanSedimentos'):
    Estaciones = Listador(Est_path, inicio='Trans',final='.csv')
else :
    Estaciones = Listador(Est_path,final='.csv')

rezagos = 24
ONI_r = pd.DataFrame([], columns=np.arange(rezagos+1))
ONI_s = pd.DataFrame([], columns=np.arange(rezagos+1))
MEI_r = pd.DataFrame([], columns=np.arange(rezagos+1))
MEI_s = pd.DataFrame([], columns=np.arange(rezagos+1))
SOI_r = pd.DataFrame([], columns=np.arange(rezagos+1))
SOI_s = pd.DataFrame([], columns=np.arange(rezagos+1))


def Correlogram(X1,X2, lags=24,
                graph=True,title='', name='Correlogram',
                pdf=False, png=True, PathFigs=Path_out):
    """
    Make correlogram with figure
    INPUTS
    X1 : array with serie to correlate
    X2 : array with serie to correlate
    lags : integer to lag the series
    """
    pear = np.empty(lags+1,  dtype=float)*np.nan
    for i in range(lags+1):
        if len(X2)-i > 3:
            pear[i]  = np.corrcoef(X1[i:],X2[:len(X2)-i])[0,1]

    if  graph == True:
        Graphs.GraphCorrelogram(pear, title=title, name=name, pdf=pdf, png=png, PathFigs=PathFigs)

    return pear


def Anomalies(Serie):
    """
    Make anomalies compared with the annual cycle
    INPUTS
    Serie : Pandas DataFrame with data
    """
    Ciclo = Cycles(Serie, type='annual')
    Devtd = Cycles(Serie, type='annual',percentiles='std')
    Anom = Serie.copy()
    for i in range(len(Serie)):
        med = Ciclo[Serie.index[i].month - 1]
        std = Devtd[Serie.index[i].month - 1]
        Anom.iloc[i] = (Serie.iloc[i] - med)/std
    return Anom

for i in range(len(Estaciones)):

    if Est_path.endswith('CleanSedimentos'):
        Est  = Estaciones[i].split('_')[1].split('.csv')[0]+'_Sedimentos'
        lab  = 'Anomalia transporte'
    elif Est_path.endswith('ENSO_P'):
        Est  = Estaciones[i].split('.csv')[0]
        lab  = 'Anomalia precipitación'
    else:
        Meta = pd.read_csv(os.path.join(Est_path, Estaciones[i].split('.')[0]+'.meta'),index_col=0)
        Name = Meta.iloc[0].values[0]
        Est  = Name+'Caudal' if Meta.iloc[-4].values[0]=='CAUDAL' else Name+'Nivel'
        lab  = 'Anomalia Caudal' if Meta.iloc[-4].values[0]=='CAUDAL' else'Anomalia Nivel'

    if Est_path.endswith('CleanNiveles'):
        Est  = Name+'NR'
        lab  = 'Anomalia nivel real'

    if Est_path.endswith('CleanSedimentos'):
        serie = pd.read_csv(os.path.join(Est_path, Estaciones[i]), index_col=0)
        serie.index = pd.DatetimeIndex(serie.index)
    elif Est_path.endswith('ENSO_P'):
        serie = pd.read_csv(os.path.join(Est_path, Estaciones[i]), index_col=0)
        serie.index = pd.DatetimeIndex(serie.index)
        serie[serie==99999] = np.nan
        serie = serie.dropna()

    else:
        serie = Read.EstacionCSV_pd(Estaciones[i], Est, path=Est_path)
        if Estaciones[i].endswith('N.csv') == False:
            serie.index = [dt.datetime.strptime(fecha.strftime("%Y-%m-%d") , "%Y-%d-%m") for fecha in serie.index]
    try:
        Anoma = Anomalies(serie)
    except:
        continue

    monthly = Anoma.groupby(lambda y : (y.year,y.month)).mean()
    monthly.index = [dt.datetime(idx[0],idx[1],1)  for idx  in monthly.index]
    monthly = monthly.dropna()
    Monthly = monthly.rolling(3).mean()
    Monthly = Monthly.dropna()
    DF_oni = Monthly.join(ONI, how='inner')
    df_oni = monthly.join(ONI, how='inner')

    DF_mei = Monthly.join(MEI, how='inner')
    df_mei = monthly.join(MEI, how='inner')

    DF_soi = Monthly.join(SOI, how='inner')
    df_soi = monthly.join(SOI, how='inner')


    Graphs.GraphSerieENSO(ONI, Anoma.sort_index(),
                          twin=False, labelENSO='ONI', labelSerie=lab, title=Est,
                          name='ONI_'+Est.replace(' ',''), pdf=False, png=True, PathFigs=Path_out)
    Graphs.GraphSerieENSO(MEI, Anoma.sort_index(),
                          twin=False, labelENSO='MEI', labelSerie=lab, title=Est,
                          name='MEI_'+Est.replace(' ',''), pdf=False, png=True, PathFigs=Path_out)
    Graphs.GraphSerieENSO(SOI, Anoma.sort_index(),
                          twin=False, labelENSO='SOI', labelSerie=lab, title=Est,
                          name='SOI_'+Est.replace(' ',''), pdf=False, png=True, PathFigs=Path_out)

    oni_r = Correlogram(DF_oni.values[:,0],DF_oni.values[:,1], lags=rezagos,
                        graph=True,
                        title=u'Correlación  ONI con '+Est,
                        name='Correlogram_ONI_'+Est.replace(' ',''),
                        pdf=False, png=True, PathFigs=Path_out)
    oni_s = Correlogram(df_oni.values[:,0],df_oni.values[:,1], lags=rezagos,
                        graph=True,
                        title=u'Correlación  ONI con '+Est,
                        name='CorrelogramSimple_ONI_'+Est.replace(' ',''),
                        pdf=False, png=True, PathFigs=Path_out)

    mei_r = Correlogram(DF_mei.values[:,0],DF_mei.values[:,1], lags=rezagos,
                        graph=True,
                        title=u'Correlación  MEI con '+Est,
                        name='Correlogram_MEI_'+Est.replace(' ',''),
                        pdf=False, png=True, PathFigs=Path_out)
    mei_s = Correlogram(df_mei.values[:,0],df_mei.values[:,1], lags=rezagos,
                        graph=True,
                        title=u'Correlación  MEI con '+Est,
                        name='CorrelogramSimple_MEI_'+Est.replace(' ',''),
                        pdf=False, png=True, PathFigs=Path_out)

    soi_r = Correlogram(DF_soi.values[:,0],DF_soi.values[:,1], lags=rezagos,
                        graph=True,
                        title=u'Correlación  SOI con '+Est,
                        name='Correlogram_SOI_'+Est.replace(' ',''),
                        pdf=False, png=True, PathFigs=Path_out)
    soi_s = Correlogram(df_soi.values[:,0],df_soi.values[:,1], lags=rezagos,
                        graph=True,
                        title=u'Correlación  SOI con '+Est,
                        name='CorrelogramSimple_SOI_'+Est.replace(' ',''),
                        pdf=False, png=True, PathFigs=Path_out)

    oni_r = pd.Series(data=oni_r, name=Est)
    oni_s = pd.Series(data=oni_s, name=Est)
    mei_r = pd.Series(data=mei_r, name=Est)
    mei_s = pd.Series(data=mei_s, name=Est)
    soi_r = pd.Series(data=soi_r, name=Est)
    soi_s = pd.Series(data=soi_s, name=Est)

    ONI_r = ONI_r.append(oni_r)
    ONI_s = ONI_s.append(oni_s)
    MEI_r = MEI_r.append(mei_r)
    MEI_s = MEI_s.append(mei_s)
    SOI_r = SOI_r.append(soi_r)
    SOI_s = SOI_s.append(soi_s)

if Est_path.endswith('CleanNiveles'):
    sufix = 'NR'
elif Est_path.endswith('CleanSedimentos'):
    sufix = 'Sed'
elif Est_path.endswith('ENSO_P'):
    sufix = 'PPT'
else:
    sufix = ''
ONI_r.to_csv(os.path.join(Path_out,f'ONIcorrelation_revisa_{sufix}.csv'))
ONI_s.to_csv(os.path.join(Path_out,f'ONIcorrelation_simple_{sufix}.csv'))
MEI_r.to_csv(os.path.join(Path_out,f'MEIcorrelation_revisa_{sufix}.csv'))
MEI_s.to_csv(os.path.join(Path_out,f'MEIcorrelation_simple_{sufix}.csv'))
SOI_r.to_csv(os.path.join(Path_out,f'SOIcorrelation_revisa_{sufix}.csv'))
SOI_s.to_csv(os.path.join(Path_out,f'SOIcorrelation_simple_{sufix}.csv'))
