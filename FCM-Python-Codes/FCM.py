#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  8 12:14:34 2019

@author: apple
"""


import pandas as pd
import numpy as np

class FCM():
    
    def dynamic_analisys(self,scenarios,weightMat,ite):
        # A set of one or more scenarios
        # A directed weight matrix
        # Number of epochs
        
        result=scenarios
        for t in scenarios:
            res=scenarios[t].values.reshape(1,-1)
            for i in range(ite):
                med=res.dot(weightMat)
                res=1/(1+np.exp(-1*(med-0.5)))
                res[med==0]=0
            result[t]=res.reshape(-1,1)
        return result        
    
    def static_analysis(self,states,targets):
        # States matrix
        # Targets matrix
        
        result=pd.DataFrame(states['Col'].values.tolist(), columns=['Col'])
        for targ in targets:
            target=targets[targ]
            concepts=pd.DataFrame(states['Col'].values.tolist(), columns=['Col'])
            concepts['target']=target.values.reshape(-1,1)
            concepts['minWithRoot']=np.zeros(len(target)).reshape(-1,1)
            concepts['rooted']=np.zeros(len(target)).reshape(-1,1)
            #start
            searchList=list(np.abs(target.unique().tolist()))
            searchList.sort(reverse=True)
            for search in searchList:
                cons = concepts[(abs(concepts['target'])==search)]
                for t in cons['Col']:
                    minMax= np.maximum(np.maximum(np.minimum(abs(states[t]),abs(search)),target),abs(concepts['minWithRoot']))
                    concepts['minWithRoot']=minMax
                    concepts['rooted']=np.maximum(abs(concepts['rooted']),concepts['minWithRoot'])
            result[targ]=concepts['rooted']
        return result
