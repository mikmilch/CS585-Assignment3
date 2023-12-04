import pandas as pd
import random
from faker import Faker


units = 100000#00
infected = random.randint((units/1000), (units/10))
# infected = random.randint(10000, 1000000)


features = ["ID", "x", "y", "Name", "Age"]
global INFECTED_small
PEOPLE_large = pd.DataFrame(columns=features)

features2 = ["ID", "x", "y", "Name", "Age", "Infected"]

PEOPLE_SOME_INFECTED_large = pd.DataFrame(columns=features2)

xlist = []
ylist = []
seats = set()

def check(x, y):
    current = str(x) + ", " + str(y)
    if (current in seats):
        newx = random.randint(1, 10000)
        newy = random.randint(1, 10000)

        check(newx, newy)
    else:
        seats.add(current)
        xlist.append(x)
        ylist.append(y)

def peopleLargeCreation():


    # generating names
    faker = Faker()
    PEOPLE_large['Name'] = [faker.name() for i in range(units)]

    # generating IDs
    PEOPLE_large["ID"] = [(i + 1) for i in range(units)]

    # generating ages
    PEOPLE_large["Age"] = [(random.randint(18,100)) for i in range(units)]

    for n in range(units):

        x = random.randint(1, 10000)
        y = random.randint(1, 10000)
        check(x, y)
    PEOPLE_large["x"] = xlist
    PEOPLE_large["y"] = ylist

    print(PEOPLE_large)
    
    PEOPLE_large.to_csv("people_large2.csv", index=False)

infectedPerson = set()




def infectedSmallCreation(large):
    INFECTED_small = pd.DataFrame(columns=features)

    INFECTED_small = large.sample(infected)

    infectedPerson.update(INFECTED_small["ID"].tolist())




    print(INFECTED_small)
    INFECTED_small.to_csv("infected_small2.csv", index=False)

def peopleSomeInfected(large):
    
    # the following columns should be the same as PEOPLE_large
    
    # ID
    PEOPLE_SOME_INFECTED_large["ID"] = large["ID"]
    
    # x and y
    PEOPLE_SOME_INFECTED_large["x"] = large["x"]
    PEOPLE_SOME_INFECTED_large["y"] = large["y"]
    
    # Name
    PEOPLE_SOME_INFECTED_large["Name"] = large["Name"]
    
    # Age 
    PEOPLE_SOME_INFECTED_large["Age"] = large["Age"]

    infectedList = []

    for i in range(len(PEOPLE_SOME_INFECTED_large)):
        if (i + 1 in infectedPerson):
            infectedList.append("Yes")
        else:
            infectedList.append("No")
        # infectedList[i] = i in infectedPerson
        print(infectedList[i])

    PEOPLE_SOME_INFECTED_large["Infected"] = infectedList


    print(PEOPLE_SOME_INFECTED_large)
    PEOPLE_SOME_INFECTED_large.to_csv("people_some_infected2.csv", index=False)



if __name__ == '__main__':


    peopleLargeCreation()
    infectedSmallCreation(PEOPLE_large)
    peopleSomeInfected(PEOPLE_large)
    