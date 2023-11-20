import pandas as pd
import random
from faker import Faker


units = 1000#0000
infected = 100 #random.randint(10000, 1000000)

features = ["ID", "x", "y", "Name", "Age"]
PEOPLE_large = pd.DataFrame(columns=features)
INFECTED_small = pd.DataFrame(columns=features)


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

infectedPerson = set()

def checkInfected(i):
    if (i in infectedPerson):
        newi = random.randint(1, units)

        checkInfected(newi)
    else:
        infectedPerson.add(i)
        return i


def infectedSmallCreation(large):

    print(large[large["ID"] == 1])
    # df2 = df2.append(df1[df1['Adj.Factor'] == 0])

    for i in range(infected):
        # print(i)
        index = checkInfected(random.randint(1, units))

        INFECTED_small = pd.concat([INFECTED_small, (large[large[["ID"] == index]])])

        # current = large[large["ID"] == index]
        #
        # INFECTED_small.insert(current)
        # print(INFECTED_small)
        # print(current)
        #
        # INFECTED_small[i]["ID"] = current["ID"]
        # INFECTED_small[i]["x"] = current["x"]
        # INFECTED_small[i]["y"] = current["y"]
        # INFECTED_small[i]["Name"] = current["Name"]
        # INFECTED_small[i]["Age"] = current["Age"]

    print(INFECTED_small)

#def peopleSomeInfectedLarge(infectedSmall, peopleLarge):
# PEOPLE_SOME_INFECTED_large["Name"] = peopleLarge["Name"]
# PEOPLE_SOME_INFECTED_large["ID"] = peopleLarge["ID"]
# PEOPLE_SOME_INFECTED_large["age"] = peopleLarge["Age"]
# PEOPLE_SOME_INFECTED_large["x"] = peopleLarge["x"]
# PEOPLE_SOME_INFECTED_large["y"] = peopleLarge["y"]

# for id in PEOPLE_SOME_INFECTED_large:
 #   if i in infectedSmall["ID"]:
#       PEOPLE_SOME_INFECTED_large[id]["Infected"] = "Yes"
#   else:
#       PEOPLE_SOME_INFECTED_large[id]["Infected"] = "No"


if __name__ == '__main__':
    peopleLargeCreation()
    infectedSmallCreation(PEOPLE_large)