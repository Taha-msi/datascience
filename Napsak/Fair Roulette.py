# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 10:55:40 2019

@author: tahamansouri
"""
import random
import statistics

class FairRoulette():
    def __init__(self):
        self.pockets=[]
        for i in range(1,37):
            self.pockets.append(i)
        self.ball=None
        self.pocketOdds = len(self.pockets)-1
    def spin(self):
        self.ball = random.choice(self.pockets)
    
    def betPocket(self, pocket, amt):
        if str(pocket) == str(self.ball):
            return amt*self.pocketOdds
        else: return -amt
        
    def __str__(self):
        return 'Fair Roulette'

class EuRoulette(FairRoulette):
    def __init__(self):
        FairRoulette.__init__(self)
        self.pockets.append('0')
    def __str__(self):
        return 'European Roulette'

class AmRoulette(EuRoulette):
    def __init__(self):
        EuRoulette.__init__(self)
        self.pockets.append('00')
        # The amounts of Odds does not change, due to these are not chance
    def __str__(self):
        return 'American Roulette'


def playRoulette(game, numSpins, pocket=2, bet=1,toPrint=False):
    totPocket = 0
    for i in range(numSpins):
        game.spin()
        totPocket += game.betPocket(pocket, bet)
    if toPrint:
        print(numSpins, ' spins of ',game)
        print('Expected return betting ', pocket, '=', str(100*totPocket/numSpins)+'%\n')
    return (totPocket/numSpins)

def findPocketReturn(game,numTrials,numSpins, toPrint):
    res=[]
    for i in range(numTrials):
        res.append(playRoulette(game, numSpins, 2,1,False))
    return res

def getMeanAndStd(results):
    return statistics.mean(results), statistics.stdev(results)
       
resultDict={}
numTrials=20
games = (FairRoulette, EuRoulette, AmRoulette)
for G in games:
    resultDict[G().__str__()]=[]

for numSpins in (1000,100000,1000000):
    print('\nSimulate betting a pocket for ', numTrials,' trials of ',numSpins, 'spins each')
    for G in games:
        pocketReturns=findPocketReturn(G(),numTrials,numSpins, False)
        mean, std = getMeanAndStd(pocketReturns)
        resultDict[G().__str__()].append((numSpins,100*mean, 100*std))
        print('Exp. return for ',G(),'=',str(round(100*mean,3))+'%',' +/-'+str(round(100*1.96*std,3))+'% with 95% confidence')
        