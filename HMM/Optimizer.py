#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 14:53:24 2019

@author: apple
"""

import numpy as np
import random
from HMM import HMM as hmm

class Optimizer:
    
    def __init__(self,O, states, observes):
        self.O=O
        self.states=states
        self.observes=observes
    
    def initializer(self):
        A=np.random.rand(self.states,self.states)
        A=A/np.sum(A, axis=1).reshape((-1, 1))
        B=np.random.rand(self.states,self.observes)
        B=B/np.sum(B, axis=1).reshape((-1, 1))
        pi=np.random.rand(1, self.states)
        pi=pi/np.sum(pi, axis=1).reshape((-1, 1))
        return A, B, pi
    
    def landaToVector(self,A,B,pi):
        vec=np.ones((1,self.states*(self.states+self.observes+1)))
        vec[0,0:self.states]=pi
        vec[0,self.states:self.states*self.states+self.states]=A.flatten()
        vec[0,self.states*self.states+self.states:]=B.flatten()
        return vec

    def vectorToLanda(self,vec):
        pi=vec[0,0:self.states].reshape(1,self.states)
        A=vec[0,self.states:self.states*self.states+self.states].reshape(self.states,self.states)
        B=vec[0,self.states*self.states+self.states:].reshape(self.states,self.observes)
        return A,B,pi 
    
    def aro(self,iter,func,pA,pB,ppi):
        parentVector=self.landaToVector(pA,pB,ppi)
        alpha=func(*[self.O, pA, pB, ppi])
        idx = np.size(parentVector)
        bud=parentVector
        probs=np.zeros((iter,1))
        for i in range(iter):
            probs[i]=alpha[-1]
            fidx= random.randint(0,idx-1)
            eidx = random.randint(fidx+1,idx)
            p= 1/(1+np.log(eidx-fidx))
            for j in range(idx):
                if (np.random.rand()<=p):
                    if(np.random.rand()<.3):
                        bud[0,j]=np.random.rand()
                    elif (np.random.rand()<.7):
                        bud[0,j]=bud[0,j]*np.random.rand()
                    else:
                        bud[0,j]=bud[0,j]*(1+np.random.rand())
            bA,bB,bpi=self.vectorToLanda(bud)
            bA=bA/np.sum(bA, axis=1).reshape((-1, 1))
            bB=bB/np.sum(bB, axis=1).reshape((-1, 1))
            bpi=bpi/np.sum(bpi, axis=1).reshape((-1, 1))
            balpha = func(*[self.O, bA, bB, bpi])
                
            if (balpha[-1]>alpha[-1]):
                parentVector = self.landaToVector(bA, bB, bpi)
                alpha=balpha
        A,B,pi=self.vectorToLanda(parentVector)       
        return [A,B,pi,probs]
    
    def aroBW(self,iter,func,pA,pB,ppi):
        inst = hmm()
        alpha=func(*[self.O, pA, pB, ppi])
        landa=[pA,pB,ppi,[]]
        probs=np.zeros((iter,1))
        P=alpha[-1]
        probs[0]=[alpha[-1]]
        for i in range(1,iter):
            landa=inst.baum_welch(self.O, landa[0], landa[1], landa[2],5)
            if (landa[-1].mean()<=P+1e-20):
                landa=self.aro(100,func,landa[0], landa[1], landa[2])
                P=landa[-1].mean()
            probs[i]=landa[-1].max()
            if (probs[i]>=1):
                print(str(i)+ "inja boodeh "+ str(probs[i]))
                return [landa[0], landa[1], landa[2],probs]
            
        return [landa[0], landa[1], landa[2],probs]       
            
     #def test(func, paramlist):
      #   return func(*paramlist)
        