#Python script is meant to replicate how mapreduce runs in Hadoop using analogy of playing cards https://www.youtube.com/watch?v=bcjSe0xCHbE
import pandas as pd
import numpy as np
import random, math

data = pd.read_csv('cards.csv', header=None, names = ['rank','suit'])
print data.head()
print data.info()
print data.describe()

#shuffle deck
data = data.iloc[np.random.permutation(len(data))]

#function for num of cards
def sampledeck(num):
	sample = data.sample(num)
	return sample

#select a number of cards
data = sampledeck(30)

#DATA MUNGING
#define function for determining whether rank is numeric
def is_numeric(val):
    if str(val).isdigit():
        return True
    else:
        return False

#add field for whether rank is numeric
data['check'] = data['rank'].apply(is_numeric)

#divide into separate stacks = analogous to splitting a file into equal chunks
heart = data[data.suit=='Hearts']
spade = data[data.suit=='Spades']
diamond = data[data.suit=='Diamonds']
club = data[data.suit=='Clubs']

#filter for ranks that are numeric (filter out Ace, King, Queen, Jack)
heart = heart[heart['check']==True]
spade = spade[spade['check']==True]
diamond = diamond[diamond['check']==True]
club = club[club['check']==True]

#convert rank from object data type to integer data type
heart['rank'] = heart['rank'].astype(int)
spade['rank'] = spade['rank'].astype(int)
diamond['rank'] = diamond['rank'].astype(int)
club['rank'] = club['rank'].astype(int)

#sort and sum
groupH = heart.groupby('suit')['rank'].sum()
groupS = spade.groupby('suit')['rank'].sum()
groupD = diamond.groupby('suit')['rank'].sum()
groupC = club.groupby('suit')['rank'].sum()

#group series into a single dataframe
groupall = pd.concat([groupH, groupS, groupD, groupC], axis=1)

#sum across dataframe to get output of 
print groupall.sum(axis=1)