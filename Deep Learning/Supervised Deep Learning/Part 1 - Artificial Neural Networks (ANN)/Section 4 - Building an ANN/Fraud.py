#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 19 00:50:56 2018

@author: Taha Mansouri
"""

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

# Importing the dataset
dataset = pd.read_csv('base_desenv_02.csv')
X_train = dataset.iloc[:, 0:17].values
y_train = dataset.iloc[:, 17].values


# Feature Scaling
from sklearn.preprocessing import StandardScaler
sc = StandardScaler()
X_train = sc.fit_transform(X_train)
#X_test = sc.transform(X_test)


import keras
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import Dropout

classifier = Sequential()

# Adding the input layer and the first hidden layer
classifier.add(Dense(units = 100, kernel_initializer = 'uniform', activation = 'relu', input_dim = 17))

# Adding the second hidden layer
classifier.add(Dense(units = 80, kernel_initializer = 'uniform', activation = 'relu'))

# Adding the third hidden layer
classifier.add(Dense(units = 80, kernel_initializer = 'uniform', activation = 'relu'))

classifier.add(Dense(units = 60, kernel_initializer = 'uniform', activation = 'relu'))

classifier.add(Dense(units = 40, kernel_initializer = 'uniform', activation = 'relu'))
# Adding the output layer
classifier.add(Dense(units = 1, kernel_initializer = 'uniform', activation = 'sigmoid'))

# Compiling the ANN
classifier.compile(optimizer = 'adam', loss = 'binary_crossentropy', metrics = ['accuracy'])

# Fitting the ANN to the Training set

classifier.fit(X_train, y_train, batch_size = 10, epochs = 20)

# Test
dataset = pd.read_csv('base_validacao_02.csv')
X_test = dataset.iloc[:, 0:17].values
y_test = dataset.iloc[:, 17].values

X_test = sc.transform(X_test)


y_pred = classifier.predict(X_test)
y_pred = (y_pred > 0.5)

# Making the Confusion Matrix
from sklearn.metrics import confusion_matrix
cm = confusion_matrix(y_test, y_pred)
acc=(cm[0][0]+cm[1][1])/len(y_pred)
print("Test accuracy is: ",acc)

cost=cm[0][0]+100*cm[0][1]+10*cm[1][0]

print("Cost is: ",cost)


