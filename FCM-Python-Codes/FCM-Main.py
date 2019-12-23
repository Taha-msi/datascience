#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 21:10:13 2019

@author: apple
"""
import pandas as pd

# Importing the dataset
dataset = pd.read_csv('ECT-Responses2.csv')
from FCM import FCM 

states = dataset.iloc[0:37, 0:38]
targets= dataset.iloc[0:37,38:41]
#wightMat= dataset.iloc[0:41,1:42]
fcm =FCM()
#scenarios = pd.read_csv('Scenarios.csv')
#scenarios=scenarios.iloc[:,1:]
#myStat= fcm.dynamic_analisys(scenarios,wightMat,2)       
myres=fcm.static_analysis(states,targets)
