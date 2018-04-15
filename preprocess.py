import pandas as pd
import numpy as np


#read data as pandas dataframe
horsedata = pd.read_csv("data/race-result-horse.csv")
orig_shape = horsedata.shape
racedata = pd.read_csv("data/race-result-race.csv")

#drop rows with non numeric "finishing_position"
horsedata = (horsedata.drop("finishing_position", axis = 1).join(horsedata["finishing_position"].apply(pd.to_numeric, errors = 'coerce')))
horsedata = horsedata[horsedata[["finishing_position"]].notnull().all(1)]

#store information when go through each row
horses = {}
horserank = {} #key-horseid, value-list of string("raceid_finishingposition")
jockeys = {} #indices
jockeyrank = [] #sum of ranks
jockeycount = [] #number of races
trainers = {}
trainerrank = []
trainercount = []
for i in range(orig_shape[0]):
    try:
        rank = horsedata.at[i, "finishing_position"]
        horse = horsedata.at[i, "horse_id"]
        jockey = horsedata.at[i, "jockey"]
        trainer = horsedata.at[i, "trainer"]
        race = horsedata.at[i, "race_id"]
        #horse
        if (str(horse) in horses):
            horserank[str(horse)].append(str(race) + "_" + str(rank))
        else:
            horses[str(horse)] = len(horses)
            horserank[str(horse)] = []
            horserank[str(horse)].append(str(race) + "_" + str(rank))
        #jockey & trainer
        if (int(race[0:4])>=2016 and int(race[5:8])>327): #testing data cannot be used
            pass
        else: #training data
            if (str(jockey) in jockeys):
                jid = jockeys[str(jockey)]
                jockeyrank[jid] += rank
                jockeycount[jid] += 1
            else:
                jockeys[str(jockey)] = len(jockeys)
                jockeyrank.append(rank)
                jockeycount.append(1)
            if (str(trainer) in trainers):
                tid = trainers[str(trainer)]
                trainerrank[tid] += rank
                trainercount[tid] += 1
            else:
                trainers[str(trainer)] = len(trainers)
                trainerrank.append(rank)
                trainercount.append(1)
            #record end of training set
            trainingend = i+1
    except:
        pass
    
race_dist = {}#key-raceid, value-distance
for i in range(racedata.shape[0]):
    race = racedata.at[i, 'race_id']
    if (race in race_dist):
        print("Error: duplicated race")
    else:
        race_dist[str(race)] = racedata.at[i, 'race_distance']

#Add "recent_6_runs" and "recent_ave_rank"
#Add "jockey_ave_rank" and "trainer_ave_rank"
#Add "race_distance"
for i in range(len(jockeyrank)):
    jockeyrank[i] = jockeyrank[i] / jockeycount[i]
for i in range(len(trainerrank)):
    trainerrank[i] = trainerrank[i] / trainercount[i]
jockey_ave = []
trainer_ave = []
rec_runs = []
rec_ave = []
dist = []
for i in range(orig_shape[0]):
    try:
        horse = horsedata.at[i, 'horse_id']
        jockey = horsedata.at[i, 'jockey']
        trainer = horsedata.at[i, 'trainer']
        
        runs = ''
        ave = 7.0
        for j in range(len(horserank[str(horse)])):
            rank = horserank[str(horse)][j].split("_")
            if(str(horsedata.at[i, 'race_id']) == rank[0]):
                k = 1
                ranksum = 0
                while(k<=6 and k<=j):
                    ranksum += float(horserank[str(horse)][j-k].split("_")[1])
                    runs = str(int(float(horserank[str(horse)][j-k].split("_")[1]))) + "/" + runs
                    k += 1
                break
        if(ranksum > 0):
            ave = ranksum / (k-1)
        if (jockey in jockeys):
            jockey_ave.append(jockeyrank[jockeys[str(jockey)]])
        else:
            jockey_ave.append(7)
        if (trainer in trainers):
            trainer_ave.append(trainerrank[trainers[str(trainer)]])
        else:
            trainer_ave.append(7)
        rec_runs.append(runs[0:len(runs)-1])
        rec_ave.append(ave)
        dist.append(race_dist[str(horsedata.at[i, 'race_id'])])
    except:
        pass

horsedata = horsedata.assign(recent_6_runs = rec_runs)
horsedata = horsedata.assign(recent_ave_rank = rec_ave)
horsedata = horsedata.assign(jockey_ave_rank = jockey_ave)
horsedata = horsedata.assign(trainer_ave_rank = trainer_ave)
horsedata = horsedata.assign(race_distance = dist)

# split data into training and testing
training = horsedata.iloc[0:trainingend, :]
testing = horsedata.iloc[trainingend:, :]
training.to_csv('training.csv')
testing.to_csv('testing.csv')










