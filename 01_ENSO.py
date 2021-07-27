#-*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import datetime as dt
import os

from Modules import Read
from Modules import Graphs
from Modules.Utils import Listador, FindOutlier, Cycles
from Modules.ENSO import SOIdata, MEIdata, ONIdata, SSTregions



Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData/PPT'))
# Est_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'CleanData/QDL'))
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
    elif Est_path.endswith('PPT'):
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

    else:
        serie = Read.EstacionCSV_pd(Estaciones[i], Est, path=Est_path)
        # if Estaciones[i].endswith('N.csv') == False:
        #     serie.index = [dt.datetime.strptime(fecha.strftime("%Y-%m-%d") , "%Y-%d-%m") for fecha in serie.index]
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

if Est_path.endswith('QDL'):
    sufix = 'QDL'
elif Est_path.endswith('CleanSedimentos'):
    sufix = 'Sed'
elif Est_path.endswith('PPT'):
    sufix = 'PPT'
else:
    sufix = ''
ONI_r.to_csv(os.path.join(Path_out,f'ONIcorrelation_revisa_{sufix}.csv'))
ONI_s.to_csv(os.path.join(Path_out,f'ONIcorrelation_simple_{sufix}.csv'))
MEI_r.to_csv(os.path.join(Path_out,f'MEIcorrelation_revisa_{sufix}.csv'))
MEI_s.to_csv(os.path.join(Path_out,f'MEIcorrelation_simple_{sufix}.csv'))
SOI_r.to_csv(os.path.join(Path_out,f'SOIcorrelation_revisa_{sufix}.csv'))
SOI_s.to_csv(os.path.join(Path_out,f'SOIcorrelation_simple_{sufix}.csv'))
