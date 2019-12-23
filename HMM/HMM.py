#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 13:56:31 2019

@author: Taha
"""
import numpy as np
import random
from datetime import datetime



class HMM:
# A: Transition matrix
# B: Emission matrix
# pi: Initial distribution
# O: Observation sequence    
    def __init__(self,states, observes):
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

    # Alpha pass
    def forward(self,O, A, B, pi):
        alpha = np.zeros((O.shape[0], A.shape[0]))
        alpha[0, :] = pi * B[:, int(O[0])]
        for t in range(1, O.shape[0]):
            for j in range(A.shape[0]):
                alpha[t, j] = alpha[t - 1].dot(A[:, j]) * B[j, int(O[t])]
        return [alpha , alpha[-1,:].sum()]
        
    # Beta pass
    def backward(self,O, A, B):
        beta = np.zeros((O.shape[0], A.shape[0]))
        beta[O.shape[0] - 1] = np.ones((A.shape[0]))
        
        # Loop in backward way from T-1 to
        # Due to python indexing the actual loop will be T-2 to 0
        for t in range(O.shape[0] - 2, -1, -1):
            for j in range(A.shape[0]):
                beta[t, j] = (beta[t + 1] * B[:, int(O[t + 1])]).dot(A[j, :])
        return beta

    #Forward backward
    def forward_backward(self, O,A,B,pi):
        alpha = self.forward(O, A, B, pi)
        beta = self.backward(O, A, B)
        gamma = alpha[0]*beta/alpha[1]
        return gamma , gamma.argmax(axis=1)

    #Baum Welsh
    def baum_welch(self, O, a, zi, pi, n_iter=10):
        M = a.shape[0]
        T = len(O)
        me=np.zeros((self.states,self.observes))
        probs=np.zeros((n_iter,1))
        start = datetime.now()
        for n in range(n_iter):
            alpha,probs[n] = self.forward(O, a, zi, pi)
            beta = self.backward(O, a, zi)
            d_gamma = np.zeros((M, M, T - 1))
            for t in range(T - 1):
                adenominator=probs[n]
                for i in range(M):
                    numerator = alpha[t, i] * a[i, :] * zi[:, int(O[t + 1])]* beta[t + 1, :]
                    d_gamma[i, :, t] = numerator / adenominator

            gamma = np.sum(d_gamma, axis=1)
            a = np.sum(d_gamma, 2) / np.sum(gamma, axis=1).reshape((-1, 1))
            # Add additional T'th element in gamma
            gamma = np.hstack((gamma, np.sum(d_gamma[:, :, T - 2], axis=0).reshape((-1, 1))))
            K = zi.shape[1]
            denominator = np.sum(gamma, axis=1)
            for l in range(K):
                for g in range(M):
                    me[g,l]=np.sum(gamma.transpose()[:,g].reshape(-1,1)[O==l])
            zi = np.divide(me, denominator.reshape(-1, 1))
            pi=gamma[:,0].reshape(1,-1)
            end=datetime.now()-start
            alpha=self.forward(O,a,zi, pi)
        
        return[a, zi, pi, alpha[-1], end, probs]
    
    def aro(self, O, A, B, pi, n_iter=100):
        parentVector=self.landaToVector(A,B,pi)
        alpha=self.forward(O,A,B, pi)
        Best=[A,B,pi,alpha[-1]]
        idx = np.size(parentVector)
        bud=parentVector
        probs=np.zeros((n_iter,1))
        start = datetime.now()
        end=datetime.now()-start
        for i in range(n_iter):
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
            balpha = self.forward(O,bA,bB, bpi)
            if (balpha[-1]>alpha[-1]):
                parentVector = self.landaToVector(bA, bB, bpi)
                alpha=balpha
                Best=[bA,bB,bpi,alpha[-1]]
                end=datetime.now()-start
        return [Best[0],Best[1],Best[2],Best[3],end,probs]


    def aroBW(self, O, A, B, pi, n_iter=100):
        alpha=self.forward(O,A,B,pi)
        probs=np.zeros((n_iter,1))
        probs[0]=[alpha[-1]]
        Best=[A,B,pi,alpha[-1]]
        start = datetime.now()
        end=datetime.now()-start
        for i in range(1,n_iter):
            landa=self.baum_welch(O, A, B, pi,5)
            A=landa[0]
            B=landa[1]
            pi=landa[2]
            alpha=landa[3]
            if (random.random()<.5):
                landa=self.aro(O,A,B,pi,100)
                A=landa[0]
                B=landa[1]
                pi=landa[2]
                alpha=landa[3]
            if (alpha>Best[-1]):
                Best=[A,B,pi,alpha]
                end=datetime.now()-start
            probs[i]=alpha
        return [Best[0],Best[1],Best[2],Best[3],end,probs]  