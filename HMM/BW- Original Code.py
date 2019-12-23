# -*- coding: utf-8 -*-
"""
Created on Tue Mar 4 10:40:47 2019

@author: IQS
"""

import gc
gc.collect()

import numpy as np
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

# Importing the dataset
#dataset = pd.read_csv('PF02171_full.csv')
#V = dataset.iloc[:, 0].values

#test sequence
test_sequence = '001120220'
test_sequence = [int(x) for x in test_sequence]
V=np.asarray(test_sequence).reshape(len(test_sequence),1)

# Transition Probabilities
a = np.ones((2, 2))
a = a / np.sum(a, axis=1)

# Emission Probabilities
b = np.array(((1, 3, 5), (2, 4, 6)))
b = b / np.sum(b, axis=1).reshape((-1, 1))

# Equal Probabilities for the initial distribution
initial_distribution = np.array((0.5, 0.5)).reshape(1,2)



# Alpha pass
def forward(V, a, b, initial_distribution):
    alpha = np.zeros((V.shape[0], a.shape[0]))
    alpha[0, :] = initial_distribution * b[:, int(V[0])]

    for t in range(1, V.shape[0]):
        for j in range(a.shape[0]):
            # Matrix Computation Steps
            #                  ((1x2) . (1x2))      *     (1)
            #                        (1)            *     (1)
            alpha[t, j] = alpha[t - 1].dot(a[:, j]) * b[j, int(V[t])]

    return alpha , alpha[-1,:].sum()

# Beta pass
def backward(V, a, b):
    beta = np.zeros((V.shape[0], a.shape[0]))

    # setting beta(T) = 1
    beta[V.shape[0] - 1] = np.ones((a.shape[0]))

    # Loop in backward way from T-1 to
    # Due to python indexing the actual loop will be T-2 to 0
    for t in range(V.shape[0] - 2, -1, -1):
        for j in range(a.shape[0]):
            beta[t, j] = (beta[t + 1] * b[:, int(V[t + 1])]).dot(a[j, :])

    return beta
#Forward backward
def forward_backward(V,a,b,initial_distribution):
    alpha,first_prob = forward(V, a, b, initial_distribution)
    beta = backward(V, a, b)
    gamma = alpha*beta/first_prob
    return gamma , gamma.argmax(axis=1)

#Baum Welsh
def baum_welch(V, a, b, initial_distribution, n_iter=10):
    M = a.shape[0]
    T = len(V)
    prob=np.zeros((n_iter,1))
    for n in range(n_iter):
        alpha,prob[n] = forward(V, a, b, initial_distribution)
        
        beta = backward(V, a, b)
        d_gamma = np.zeros((M, M, T - 1))
        for t in range(T - 1):
            adenominator=prob[n]
            for i in range(M):
                numerator = alpha[t, i] * a[i, :] * b[:, int(V[t + 1])]* beta[t + 1, :]
                d_gamma[i, :, t] = numerator / adenominator

        gamma = np.sum(d_gamma, axis=1)
        a = np.sum(d_gamma, 2) / np.sum(gamma, axis=1).reshape((-1, 1))

        # Add additional T'th element in gamma
        gamma = np.hstack((gamma, np.sum(d_gamma[:, :, T - 2], axis=0).reshape((-1, 1))))

        K = b.shape[1]
        denominator = np.sum(gamma, axis=1)
        for l in range(K):
            for g in range(M):
                b[g,l]=np.sum(gamma.transpose()[:,g].reshape(T,1)[V==l])

        b = np.divide(b, denominator.reshape(-1, 1))
        
        initial_distribution=gamma[:,0].reshape(1,2)

    return a, b, initial_distribution,prob

n_iter=150
a, b, initial_distribution,prob=baum_welch(V, a, b, initial_distribution, n_iter)
plt.plot(range(n_iter),prob)
plt.show()
