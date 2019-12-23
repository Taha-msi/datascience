#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 16 01:00:58 2018

@author: apple
"""

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

dataset = pd.read_csv('WBCD.csv')

X = dataset.iloc[:, 2: ].values
y = dataset.iloc[:, 1].values

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)


from sklearn.preprocessing import StandardScaler
sc = StandardScaler()
X_train = sc.fit_transform(X_train)
X_test = sc.transform(X_test)


import keras
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import Dropout



classifier = Sequential()

classifier.add(Dense(units = 30, kernel_initializer = 'uniform', activation = 'relu', input_dim = 30))
#classifier.add(Dropout(p=0.1))
classifier.add(Dense(units = 30, kernel_initializer = 'uniform', activation = 'relu'))
#classifier.add(Dropout(p=0.1))
classifier.add(Dense(units = 1, kernel_initializer = 'uniform', activation = 'sigmoid'))

classifier.compile(optimizer = 'adam', loss = 'binary_crossentropy', metrics = ['accuracy'])
classifier.fit(X_train, y_train, batch_size = 10, epochs = 100)

y_pred = classifier.predict(X_test)
y_pred = (y_pred > 0.5)

from sklearn.metrics import confusion_matrix
cm = confusion_matrix(y_test, y_pred)

acc=(cm[0][0]+cm[1][1])/114

print("accuracy is: ",acc)