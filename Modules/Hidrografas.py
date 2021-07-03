#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import numpy as np
import pandas as pd
import datetime as dt

import Graphs

Path = os.getcwd()

def kmsq2misq(km2):
    """
    Pass square kms to square milles
    INPUTS
    km2 : float of square kms
    """
    if km2 is not None:
        return km2/2.58998811034
    else:
        return None

def km2mi(km):
    """
    Pass kms to milles
    INPUTS
    km : float of kms
    """
    if km is not None:
        return km/1.609
    else:
        return None
def km2ft(km):
    """
    Pass kms to foot
    INPUTS
    km : float of kms
    """
    if km is not None:
        return (km/1.609)*5280
    else:
        return None
def m2ft(m):
    """
    Pass metres to foot
    INPUTS
    km : float of metres
    """
    if m is not None:
        return m*3.28084
    else:
        return None

HUFF = np.array([[0,50,85,93,95,97,98,99,100,100,100],
                 [0,49,75,88,92,95,97,98, 99,100,100],
                 [0,30,65,82,89,92,95,97, 97, 99,100],
                 [0,22,58,78,85,90,93,95, 96, 98,100],
                 [0,18,50,71,80,85,90,94, 95, 98,100],
                 [0,14,45,65,75,80,85,91, 94, 97,100],
                 [0,11,40,58,68,73,79,87, 92, 96,100],
                 [0, 8,35,51,59,66,72,80, 90, 95,100],
                 [0, 5,28,45,52,57,63,71, 82, 93,100]])
class Hidrografa:
    """
    Class to calc unitarian hydrograph
    INPUTS
    Area            : Área de drenaje de la cuenca [kmˆ2]
    Perimetro       : Perímetro de la cuenca [km]
    CN_II           : Numero de la curva
    Long_Cause      : Longitud del cause principal [km]
    Long_RioDivi    : Longitud río hasta la divisoria [km]
    Pend_Cause      : Pendiente del cause principal [%]
    Pend_Cuenca     : Pendiente de la cuenca [%]
    Cota_MaxCuenca  : Cota mayor de la cuenca [m]
    Cota_MinCuenca  : Cota menor de la cuenca [m]
    Cota_MaxRio     : Cota mayor del río [m]
    Cota_MinRio     : Cota menor del río [m]
    Long_Centroide  : Longitud del cause al centroide [km]
    Long_Cuenca     : Longitud al punto más alejado, longitud de la cuenca [km]
    Enlong_Ratio    : Relación de enlongación
    Horton_FormFact : Factor de forma de Horton
    Compacidad_Coef : Coeficiente de compacidad
    TR              : List or array with return times
    IDF             : List or array with Intensity asociated with return times
    K1_TR           : List or array with  constants k valid for duratios < 105
    K2_TR           : List or array with  constants k valid for duratios > 105
    t_dist          : List or array with time distribution
    Prob_Huff       : Float of probability of Huff distribution
    """
    def __init__(self, Area=None, Perimetro=None, CN_II=None,
                 Long_Cause=None, Long_RioDivi=None,
                 Pend_Cause=None, Pend_Cuenca=None,
                 Cota_MaxCuenca=None, Cota_MinCuenca=None,
                 Cota_MaxRio=None, Cota_MinRio=None,
                 Long_Centroide=None, Long_Cuenca=None,
                 Enlong_Ratio=None, Horton_FormFact=None, Compacidad_Coef=None,
                 TR=None, IDF=None,t_dist=None, Prob_Huff=None,K1_TR=None, K2_TR=None, ):

        self.Area_km = Area
        self.Area_mi = kmsq2misq(Area)

        self.Perim  = Perimetro
        self.CN_II = CN_II
        self.LongCause_km = Long_Cause
        self.LongCause_ft = km2ft(Long_Cause)
        self.LongCause_mi = km2mi(Long_Cause)

        self.LongCuenca_km = Long_Cuenca
        self.LongCuenca_ft = km2ft(Long_Cuenca)
        self.LongCuenca_mi = km2mi(Long_Cuenca)

        self.LongRioDivi_km = Long_RioDivi
        self.LongRioDivi_ft = km2ft(Long_RioDivi)

        self.PendCause  = Pend_Cause
        self.PendCuenca = Pend_Cuenca

        self.HmaxCuenca_m  = Cota_MaxCuenca
        self.HmaxCuenca_ft = m2ft(Cota_MaxCuenca)

        self.HminCuenca_m  = Cota_MinCuenca
        self.HminCuenca_ft = m2ft(Cota_MinCuenca)

        self.HmaxRio_m  = Cota_MaxRio
        self.HmaxRio_ft = m2ft(Cota_MaxRio)

        self.HminRio_m  = Cota_MinRio
        self.HminRio_ft = m2ft(Cota_MinRio)

        self.Centroide = Long_Centroide
        self.Enlong = Enlong_Ratio
        self.HortonFF = Horton_FormFact
        self.Compacidad = Compacidad_Coef

        self.IDF = IDF

        if TR is None:
            self.TR = np.array([2.33,5,10,25,50,100])
        else:
            self.TR = TR

        # if K1_TR is None:
        #     self.K1 = np.array([31.87,37.08,41.00,45.64,48.91,52.06])
        # else:
        #     self.K1 = K1_TR
        #
        # if K2_TR is None:
        #     self.K2 = np.array([1664.90,1937.23,2142.03,2384.33,2555.25,2719.43])
        # else:
        #     self.K2 = K2_TR

        if Prob_Huff is None:
            self.Prob_Huff = 50
        else:
            self.Prob_Huff = Prob_Huff

        if t_dist is None:
            # self.t_dist = np.array([0,16,52,69,80,85,87,91,94,98,100])
            self.t_dist = HUFF[int(self.Prob_Huff/10 -1)]
        else:
            self.t_dist = t_dist

    def SCS(self, tiempos=None, Tc=None,
            graph=False, title_fig='', name_fig='Hidrografa_SCS',
            pdf_out=True, png_out=False, Path_Figs=Path,):
        """
        Calcula hidrogrfa  unitaria del SCS
        INPUTS
        tiempos  : array with times to interpolate the flow
        graph    : Boolean to do grahpic
        title_fig: Figure title
        name_fig : Name to save figure
        pdf_out  : Boolean to save figure in pdf format
        png_out  : Boolean to save figure in png format
        PathFigs : Aboslute route to directory where figure will be save

        """
        if Tc is None:
            K = 3.28084*(self.LongCause_km**3)/(self.HmaxRio_ft - self.HminRio_ft)
            self.tc_SCS = 0.97*(K**0.385)
        else:
            self.tc_SCS = Tc

        self.Tr_SCS = self.tc_SCS*3/5
        self.T_SCS = 0.133*self.tc_SCS
        self.Tp_SCS = self.T_SCS/2. + self.Tr_SCS
        self.Up_SCS = (484*self.Area_mi/self.Tp_SCS)/(25.4*35.315)
        Adim= np.array([
        (0.0,0.000),(0.1,0.030),(0.2,0.100),(0.3,0.190),(0.4,0.310),(0.5,0.470),
        (0.6,0.660),(0.7,0.820),(0.8,0.930),(0.9,0.990),(1.0,1.000),(1.1,0.990),
        (1.2,0.930),(1.3,0.860),(1.4,0.780),(1.5,0.680),(1.6,0.560),(1.7,0.460),
        (1.8,0.390),(1.9,0.330),(2.0,0.280),(2.2,0.207),(2.4,0.147),(2.6,0.107),
        (2.8,0.077),(3.0,0.055),(3.2,0.040),(3.4,0.029),(3.6,0.021),(3.8,0.015),
        (4.0,0.011),(4.5,0.005),(5.0,0.000)
        ])

        self.t_SCS = self.Tp_SCS*Adim[:,0]
        self.Q_SCS = self.Up_SCS*Adim[:,1]

        if graph == True:
            Graphs.GraphHydrografa(self.t_SCS,self.Q_SCS, title_fig, name_fig, pdf_out, png_out, Path_Figs)
        if tiempos is None:
            return self.t_SCS ,self.Q_SCS
        else:
            return np.interp(tiempos, self.t_SCS ,self.Q_SCS)

    def Sneyder(self, tiempos=None, Tc= None, Cp=0.8,
                graph=False, title_fig='', name_fig='Hidrografa_Sneyder',
                pdf_out=True, png_out=False, Path_Figs=Path,):
        """
        Calcula hidrogrfa  unitaria del Sneyder
        INPUTS
        tiempos  : array with times to interpolate the flow
        Tc       : concentration time
        Cp       : bolean between 0.5 to 0.8
        graph    : Boolean to do grahpic
        title_fig: Figure title
        name_fig : Name to save figure
        pdf_out  : Boolean to save figure in pdf format
        png_out  : Boolean to save figure in png format
        PathFigs : Aboslute route to directory where figure will be save

        """
        if Tc is None:
            K = 3.28084*(self.LongCause_km**3)/(self.HmaxRio_ft - self.HminRio_ft)
            self.tc_Sny = 0.97*(K**0.385)
        else:
            self.tc_Sny = Tc
        self.Tr_Sny = self.tc_Sny*3/5
        self.Ts_Sny = self.Tr_Sny/5.5
        self.Tp_Sny = self.tc_Sny/2. + self.Tr_Sny

        self.u_Sny = Cp*640/(self.Tr_Sny+(self.tc_Sny-self.Ts_Sny)/4)
        self.Up_Sny = (self.Area_mi*self.u_Sny)/(25.4*35.315)

        self.Tb_Sny = 3+3*(self.Tr_Sny/24)
        if self.Tb_Sny > 3:
            self.Tb_Sny = 4*self.Tp_Sny

        self.W50_Sny = 770/(self.u_Sny**1.08)
        self.W75_Sny = 440/(self.u_Sny**1.08)

        self.t_Sny = np.array([0,
                               self.Tp_Sny - self.W50_Sny/3,
                               self.Tp_Sny - self.W75_Sny/3,
                               self.Tp_Sny,
                               self.Tp_Sny + 2*self.W75_Sny/3,
                               self.Tp_Sny + 2*self.W50_Sny/3,
                               self.Tb_Sny
                               ])
        self.Q_Sny = np.array([0,
                               self.Up_Sny*0.5,
                               self.Up_Sny*0.75,
                               self.Up_Sny,
                               self.Up_Sny*0.75,
                               self.Up_Sny*0.5,
                               0
                               ])

        if graph == True:
            Graphs.GraphHydrografa(self.t_Sny,self.Q_Sny, title_fig, name_fig, pdf_out, png_out, Path_Figs)
        if tiempos is None:
            return self.t_Sny ,self.Q_Sny
        else:
            return np.interp(tiempos, self.t_Sny ,self.Q_Sny)

    def WilliansHann(self, tiempos=None, Tc=None,
                     graph=False, title_fig='', name_fig='Hidrografa_WilliamsHann',
                     pdf_out=True, png_out=False, Path_Figs=Path,):
        """
        Calcula hidrogrfa  unitaria del Willians & Hann
        INPUTS
        tiempos  : array with times to interpolate the flow
        Tc       : concentration time
        graph    : Boolean to do grahpic
        title_fig: Figure title
        name_fig : Name to save figure
        pdf_out  : Boolean to save figure in pdf format
        png_out  : Boolean to save figure in png format
        PathFigs : Aboslute route to directory where figure will be save
        """
        Wc = self.Area_mi/self.LongCuenca_mi
        Sc = (self.HmaxRio_ft-self.HminRio_ft)/self.LongCause_mi

        self.K_WH = 27*(self.Area_mi**0.231)*(Sc**-0.777)*((self.LongCuenca_mi/Wc)**0.124)
        if Tc is None:
            self.Tp_WH = 4.63*(self.Area_mi**0.422)*(Sc**-0.48)*((self.LongCuenca_mi/Wc)**0.133)
        else:
            self.Tp_WH = Tc

        frac = self.K_WH/self.Tp_WH

        n = 1 + (1/(2*frac)+((1/(4*(frac**2))+1/frac)**0.5))**2
        if n <= 1.27:
            y = np.poly1d([7527.3397824, - 28318.9594289, 35633.3593146,-14897.3755403])
        # elif n <= 12:
        else:
            y = np.poly1d([-0.0053450748, 0.1120132788, -0.1735395123, -12.7945848518, 163.3452557299,-85.1829993108])

        self.B_WH = y(n)

        self.t0_WH = self.Tp_WH*(1+ (1/((n-1)**0.5)))
        self.t1_WH = self.t0_WH+ 2*self.K_WH

        # self.Up_WH = (self.B_WH*self.Area_mi/self.Tp_WH)/(25.4*35.315)/25.4
        self.Up_WH = (self.B_WH*self.Area_mi/self.Tp_WH)*((0.3048)**3)*(1/25.4)
        self.U0_WH = self.Up_WH * ((self.t0_WH/self.Tp_WH)**(n-1)) * np.exp((1-n)*((self.t0_WH/self.Tp_WH)-1))
        self.U1_WH = self.U0_WH * np.exp((self.t0_WH-self.t1_WH)/self.K_WH)


        tr1 = np.arange(0,self.Tp_WH, self.Tp_WH/10.)
        tr2 = np.arange(self.Tp_WH, self.t0_WH, self.Tp_WH/10.)
        tr3 = np.arange(self.t0_WH, self.t1_WH+self.Tp_WH, self.Tp_WH/10.)
        self.t_WH = np.concatenate((tr1,tr2,tr3), axis=None)

        q = np.zeros(self.t_WH.shape, dtype=float)
        for i in range(len(self.t_WH)):
            if self.t_WH[i] <= self.t0_WH :
                q[i] = self.Up_WH * ((self.t_WH[i]/self.Tp_WH)**(n-1)) * np.exp((1-n)*((self.t_WH[i]/self.Tp_WH)-1))
            elif self.t_WH[i] <= self.t1_WH:
                q[i] = self.U0_WH * np.exp((self.t0_WH-self.t_WH[i])/self.K_WH)
            else:
                q[i] = self.U1_WH * np.exp((self.t1_WH-self.t_WH[i])/(2*self.K_WH))

        self.Q_WH = q

        if graph == True:
            Graphs.GraphHydrografa(self.t_WH,self.Q_WH, title_fig, name_fig, pdf_out, png_out, Path_Figs)
        if tiempos is None:
            return self.t_WH ,self.Q_WH
        else:
            return np.interp(tiempos, self.t_WH ,self.Q_WH)

    def Tc_Kirpich(self):
        """
        Calculate concentration time of Kirpich (1990)
        """
        self.tc_Kirpich = 0.066*((self.LongCause_km/((self.PendCause/100)**0.5))**0.77)

        return self.tc_Kirpich


    def Tc_Temez(self):
        """
        Calculate concentration time of Temez (1978)
        """
        self.tc_Temez = 0.3*((self.LongCause_km/(self.PendCause**0.25))**0.76)

        return self.tc_Temez


    def Tc_Giandoti(self):
        """
        Calculate concentration time of Giandoti (1990)
        """
        self.tc_Giandoti = (4*(self.Area_km**0.5)+ 1.5*self.LongCause_km)/(25.3*((self.LongCause_km*self.PendCause/100)**0.5))

        return self.tc_Giandoti


    def Tc_Williams(self):
        """
        Calculate concentration time of Williams
        """
        self.tc_Williams = 0.272*self.LongCause_km*(self.Area_km**0.4)/(((self.PendCause/100)**0.2)*((4*self.Area_km/np.pi)**0.5))

        return self.tc_Williams


    def Tc_Johnstone(self):
        """
        Calculate concentration time of Johnstone (1949)
        """
        # self.tc_Johnstone = 5*self.LongCause_mi/(((self.PendCause/100)*1609/3.281)**0.5)
        self.tc_Johnstone = 5*self.LongCause_mi/(((self.HmaxRio_ft-self.HminRio_ft)/self.LongCause_mi)**0.5)

        return self.tc_Johnstone


    def Tc_California(self):
        """
        Calculate concentration time of California Culverts Practice (1942)
        """
        self.tc_California = ((0.87075*(self.LongCause_km**3)/(self.HmaxCuenca_m - self.HminCuenca_m))**0.385)
        return self.tc_California


    def Tc_Clark(self):
        """
        Calculate concentration time of Clark
        """
        self.tc_Clark = 0.0335*((self.Area_km/((self.PendCause/100)**0.5))**0.593)
        return self.tc_Clark


    def Tc_Passinni(self):
        """
        Calculate concentration time of Passinni
        """
        self.tc_Passinni = 0.108*((self.LongCause_km*self.Area_km)**(1/3))/(((self.PendCause/100)**0.5))

        return self.tc_Passinni


    def Tc_Pilgrim(self):
        """
        Calculate concentration time of Pilgrim
        """
        self.tc_Pilgrim = 0.76*(self.Area_km**0.38)

        return self.tc_Pilgrim


    def Tc_SCS(self):
        """
        Calculate concentration time of SCS
        """
        self.tc_SCS = 0.947*(((self.LongCause_km**3)/(self.HmaxRio_m-self.HminRio_m))**0.385)

        return self.tc_SCS


    def Tc_Valencia(self):
        """
        Calculate concentration time of Valencia
        """
        self.tc_Valencia = 1.7694*(self.Area_km**0.325)*(self.LongCause_km**-0.096)*(self.PendCause**-0.29)

        return self.tc_Valencia


    def Tc_Bransby(self):
        """
        Calculate concentration time of Bransby
        """
        self.tc_Bransby = (1/60.)*14.6*self.LongCause_km/((self.Area_km**0.1)*((self.PendCause/100)**0.2))

        return self.tc_Bransby

    def ConcentrationTimes(self,
                           graph=False,title_fig='',
                           name_fig='ConcentrationTimes',
                           pdf_out=True, png_out=False, Path_Figs=Path,):
        """
        Calculate concentration time with all methodologies
        INPUTS
        graph    : Boolean to do grahpic
        title_fig: Figure title
        name_fig : Name to save figure
        pdf_out  : Boolean to save figure in pdf format
        png_out  : Boolean to save figure in png format
        PathFigs : Aboslute route to directory where figure will be save
        """

        self.Tc = {'Kirpich'    : self.Tc_Kirpich(),
                   'Temez'      : self.Tc_Temez(),
                   'Giandoti'   : self.Tc_Giandoti(),
                   'Williams'   : self.Tc_Williams(),
                   'Johnstone'  : self.Tc_Johnstone(),
                   'California' : self.Tc_California(),
                   'Clark'      : self.Tc_Clark(),
                   'Passinni'   : self.Tc_Passinni(),
                   'Pilgrim'    : self.Tc_Pilgrim(),
                   'SCS'        : self.Tc_SCS(),
                   'Valencia'   : self.Tc_Valencia(),
                   'Bransby'    : self.Tc_Bransby(),
                   }

        vals = np.array(list(self.Tc.values()))
        lims = np.percentile(vals, [25,75])

        idx = np.where((vals>lims[0])& (vals<lims[1]))[0]

        self.Tc_mean = np.mean(vals[idx])
        if graph == True:
            Graphs.GraphTc(self.Tc, title_fig, name_fig, pdf_out, png_out, Path_Figs)

        self.Tc.update({'MEAN':self.Tc_mean})
        return self.Tc


    #
    # def IDF(self, Tr=None, K1=None, K2=None,
    #         graph=False, cmap_name='jet',
    #         name_fig='IDF', pdf_out=True, png_out=False, Path_Figs=Path):
    #     """
    #     Calculate IDF
    #     INPUTS
    #     Tr       : List or array with return times
    #     K1       : List or array with  constants k valid for duratios < 105
    #     K2       : List or array with  constants k valid for duratios > 105
    #     graph    : Boolean to do grahp
    #     cmap_name: name of cmap
    #     name_fig : Name to save figure
    #     pdf_out  : Boolean to save figure in pdf format
    #     png_out  : Boolean to save figure in png format
    #     PathFigs : Aboslute route to directory where figure will be save
    #     """
    #     if Tr is None:
    #         Tr = self.TR
    #     if K1 is None:
    #         K1 = self.K1
    #     if K2 is None:
    #         K2 = self.K2
    #     min_duration = 5
    #     max_duration = 1440
    #     d1 = np.tile(np.arange(min_duration,105,1), (len(Tr),1))
    #     d2 = np.tile(np.arange(105,max_duration+1,1), (len(Tr),1))
    #     duration = np.arange(min_duration,max_duration+1,1)
    #     I1 = np.zeros(d1.shape)
    #     I2 = np.zeros(d2.shape)
    #     for i in range(len(Tr)):
    #         I1[i,:] = K1[i]*(46.2/(d1[i,:]**(0.75))- 43.05/d1[i,:] )
    #         I2[i,:] = K2[i]*(d2[i,:]**(-0.85))
    #
    #     I = np.column_stack((I1,I2))
    #     I = np.rollaxis(I, 1,0)
    #
    #     if graph == True:
    #         Graphs.GraphIDF(I, duration, Tr, cmap_name, name_fig, pdf_out, png_out, Path_Figs)
    #     return I, duration, Tr
    #
    #
    # def IDF_value(self, duration, Tr=None, K1=None, K2=None,):
    #     """
    #     Calculate IDF single value
    #     INPUTS
    #     duration : Float of duration [min]
    #     Tr       : List or array with return times
    #     K1       : List or array with  constants k valid for duratios < 105
    #     K2       : List or array with  constants k valid for duratios > 105
    #     """
    #     if Tr is None:
    #         Tr = self.TR
    #     if K1 is None:
    #         K1 = self.K1
    #     if K2 is None:
    #         K2 = self.K2
    #
    #
    #     if (duration >=5 )&(duration <105):
    #         I = K1*(46.2/(duration**(0.75))- 43.05/duration )
    #     elif (duration >= 105) & (duration <= 1440):
    #         I = K2*(duration**(-0.85))
    #     else:
    #         raise Exception("duration must be in the interval [5,1440]")
    #
    #     return I

    def PPT_total(self, Tc, Tr=None, t_rule=None):
        """
        Calculate the total precipitation given concentration time for the return times given
        INPUTS
        Tc     : Concetration time [min]
        Tr     : List or array with return times
        t_rule :List or array with time distribution
        """
        if t_rule is None:
            t_rule = self.t_dist

        T_acum = t_rule/100.
        # Int = self.IDF_value(Tc, Tr)
        Int = self.IDF
        PPT = Int*Tc
        P_acum = np.zeros((len(T_acum), len(PPT)),dtype=float)
        P_tota = np.zeros((len(T_acum), len(PPT)),dtype=float)

        for i in range(len(T_acum)):
            P_acum[i,:] = PPT*T_acum[i]
            if i != 0:
                P_tota[i,:] = P_acum[i,:] - P_acum[i-1,:]

        return P_acum, P_tota

    def Loses_SCS(self, Tc, Tr=None, t_rule=None):
        """
        Calculate the loses SCS for a given concentration time for the return times given
        INPUTS
        Tc     : Concetration time [min]
        Tr     : List or array with return times
        t_rule :List or array with time distribution
        """
        self.CN_III = 23*self.CN_II/(10+0.13*self.CN_II)

        S  = 25.4*((1000/self.CN_III )-10)
        la = 0.2*S

        P_acum, P_tota = self.PPT_total(Tc, Tr)

        Pe_acum = np.zeros(P_acum.shape, dtype=float)
        Pe_tota = np.zeros(P_tota.shape, dtype=float)

        for  i in range(Pe_acum.shape[0]):
            for j in range(Pe_acum.shape[1]):
                if (P_acum[i,j]-la) > 0:
                    Pe_acum[i,j] = ((P_acum[i,j]-la)**2)/(P_acum[i,j]-la+S)

            if i !=0:
                Pe_tota[i,:] = Pe_acum[i,:] - Pe_acum[i-1,:]

        return Pe_acum, Pe_tota


    def Hietogram(self, Tc, Tr=None, t_rule=None,
                  graph=False, title_fig='',name_fig='Hietogram',
                  pdf_out=True, png_out=False, Path_Figs=Path):

        """
        Make precipitation hietogram
        INPUTS
        Tc       : Concetration time [min]
        Tr       : List or array with return times
        t_rule   : List or array with time distribution
        graph    : Boolean to do grahpic
        name_fig : Name to save figure
        pdf_out  : Boolean to save figure in pdf format
        png_out  : Boolean to save figure in png format
        PathFigs : Aboslute route to directory where figure will be save
        """
        if Tr is None:
            Tr = self.TR
        if t_rule is None:
            t_rule = self.t_dist

        self.P_acum, self.P_tota  = self.PPT_total(Tc,Tr,t_rule)
        self.Pe_acum, self.Pe_tota = self.Loses_SCS(Tc,Tr,t_rule)
        t = Tc*np.linspace(0,1, self.P_tota.shape[0])
        if graph == True:
            Graphs.GraphHietogram(self.P_tota, self.Pe_tota, np.around(t,1), Tr, title_fig,name_fig, pdf_out, png_out, Path_Figs)
        return self.P_acum, self.P_tota, self.Pe_acum, self.Pe_tota

    def Hydrogram(self,Tc, Time, U_Hidrograph, Tr=None, t_rule=None,
                  graph=False, join=True, title_fig='', name_fig='Hydrogram',
                  cmap_name='jet',pdf_out=True, png_out=False, Path_Figs=Path):
        """
        Make total Hidrogram
        INPUTS
        Tc          : Concentratio time
        Time        :
        U_Hidrograph: Unitarian hydrograph
        Tr          : List or array with return times
        t_rule      : List or array with time distribution
        graph       : Boolean to do grahpic
        title_fig   : Figure title
        name_fig    : Name to save figure
        cmap_name   : color map name
        pdf_out     : Boolean to save figure in pdf format
        png_out     : Boolean to save figure in png format
        PathFigs    : Aboslute route to directory where figure will be save
        """
        if Tr is None:
            Tr = self.TR
        if t_rule is None:
            t_rule = self.t_dist

        Pe_a, Pe_t = self.Loses_SCS(Tc,Tr,t_rule)
        n = 3
        C1 = np.zeros((Pe_a.shape[0]*(n+1)-1, Pe_a.shape[0]*n,len(Tr)),dtype=float)
        C1[:Pe_a.shape[0],0,:] = Pe_t
        for i in range(n*Pe_a.shape[0]-1):
            C1[:,i+1,:] = np.roll(C1[:,i,:],1,axis=0)

        Q = np.zeros(C1.shape[0], dtype=float)
        Q[Pe_a.shape[0]-1:Pe_a.shape[0]+U_Hidrograph.shape[0]-1] = U_Hidrograph

        C2 = np.zeros(C1.shape,dtype=float)
        for i in range(C1.shape[1]):
            for j in range(C1.shape[2]):
                C2[:,i,j] = Q*C1[:,i,j]

        self.H = np.sum(C2,axis=0)
        # self.t_hydrogram = np.arange(0,self.H.shape[0])*(Tc/(len(Time)-1))/60  #hours
        self.t_hydrogram = np.arange(0,self.H.shape[0])*(Tc/(len(Time)-1))  #hours
        if graph == True:
            Graphs.GraphHydrogram(self.t_hydrogram, self.H, Tr, join, title_fig, name_fig, cmap_name, pdf_out, png_out, Path_Figs)

        return self.H, self.t_hydrogram


    def Qmax(self, Tc=None, Time=None,
             graph=False, title_fig='', name_fig='MaxFlow',
             pdf_out=True, png_out=False, Path_Figs=Path,):
        """
        Calculate Maximum flow foe each return time
        INPUTS
        Tc
        """
        if Tc is None:
            Tc = self.Tc_mean

        if Time is None:
            Time = np.linspace(0,Tc,11) # make vector with 10% delta, can be any delta


        h_SCS, t_hydrogram = self.Hydrogram(Tc, Time, self.SCS(tiempos=Time,Tc=Tc))
        h_Sny, t_hydrogram = self.Hydrogram(Tc, Time, self.Sneyder(tiempos=Time,Tc=Tc))
        h_Wil, t_hydrogram = self.Hydrogram(Tc, Time, self.WilliansHann(tiempos=Time,))

        self.Qmax_SCS = np.max(h_SCS,axis=0)
        self.Qmax_Sny = np.max(h_Sny,axis=0)
        self.Qmax_Wil = np.max(h_Wil,axis=0)

        if graph == True:
            Graphs.GraphQmax(self.TR, self.Qmax_SCS, self.Qmax_Sny, self.Qmax_Wil,title_fig, name_fig, pdf_out, png_out, Path_Figs )

        return {'SCS': self.Qmax_SCS, 'Sny':self.Qmax_Sny, 'Wil':self.Qmax_Wil}
