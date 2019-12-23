#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 14:27:40 2019

@author: Taha Mansouri
"""

import gc
gc.collect()

import numpy as np
import matplotlib.pyplot as plt
from HMM import HMM as hmm


#test sequence
test_sequence = [0,1,2,1,0,3,4,4,1,3,5,5,6,2,7,8,1,0,4,7,4,4,9,5,3,2,7,10,0,0,9,6,8,6]
O=np.asarray(test_sequence).reshape(len(test_sequence),1)


inst = hmm(6,11)

A,B,pi=inst.initializer()

iter=500

landa_BW=inst.baum_welch(O, A,B, pi,iter)
landa_ARO=inst.aro(O, A, B, pi, iter)
landa_AROBW=inst.aroBW(O, A, B, pi, iter)
 
# multiple line plot
plt.plot( range(iter), landa_BW[-1], marker='', markerfacecolor='blue', markersize=3, color='skyblue', linewidth=4)
plt.plot( range(iter), landa_ARO[-1], marker='', color='olive', linewidth=2)
plt.plot( range(iter), landa_AROBW[-1], marker='', color='olive', linewidth=2, linestyle='dashed', label="toto")
plt.legend(['Baum Welsh', 'ARO', 'ARO Baum Welsh'], loc='upper left')
plt.show()

response={"BW":[landa_BW[3],landa_BW[4]],"ARO":[landa_ARO[3],landa_ARO[4]],"AROBW":[landa_AROBW[3],landa_AROBW[4]]}
intrep=inst.forward(O,A,B,pi)
print("initial response: ",intrep[-1])
print("BW", response["BW"])
print("ARO", response["ARO"])
print("AROBW", response["AROBW"])
