import pandas as pd
import random
from faker import Faker

#The Customers (C) dataset should have the following attributes for each customer:
# ID: unique sequential number (integer) from 1 to 50,000 (50,000 lines)
# Name: random sequence of characters of length between 10 and 20 (careful: no
# commas)
# Age: random number (integer) between 18 to 100
# CountryCode: random number (integer) between 1 and 500
# Salary: random number (float) between 100 and 10,000,000

customers = 50000

features = ["ID", "Name", "Age", "CountryCode", "Salary"]
Customers = pd.DataFrame(columns=features)
faker = Faker()


def customerGeneration():

    Customers["ID"] = [(i + 1) for i in range(customers)]

    Customers["Name"] = [faker.name() for i in range(customers)]

    Customers["Age"] = [random.randint(18, 100) for i in range(customers)]

    Customers["CountryCode"] = [random.randint(1, 500) for i in range(customers)]

    Customers["Salary"] = [random.uniform(100, 10000000) for i in range(customers)]

    print(Customers)

    Customers.to_csv("CustomersTest.csv", index=False)



# The Purchases (P) dataset should have the following attributes for each purchase
# transaction:
# TransID: unique sequential number (integer) from 1 to 5,000,000 (5M purchases)
# CustID: References one of the customer IDs, i.e., on Avg. a customer has 100
# transactions.
# TransTotal: Purchase amount as random number (float) between 10 and 2000
# TransNumItems: Number of items as random number (integer) between 1 and 15
# TransDesc: Text of characters of length between 20 and 50 (careful: no commas)

features2 = ["TransID", "CustID", "TransTotal", "TransNumItems", "TransDesc"]

transactions = 5000000

Transactions = pd.DataFrame(columns=features2)

def transactionGeneration():
    # generating TransIDs
    Transactions["TransID"] = [(i + 1) for i in range(transactions)]

    # CustID
    Transactions["CustID"] = [random.randint(1, customers) for i in range(transactions)]

    # TransTotal
    Transactions["TransTotal"] = [random.uniform(10, 2000) for i in range(transactions)]

    # TransNumItems
    Transactions["TransNumItems"] = [random.randint(1, 15) for i in range(transactions)]

    # TransDesc

    list = [
        "Stainless Steel Water Bottle",
        "Wireless Bluetooth Earbuds",
        "Portable Phone Charger",
        "Non-Stick Cooking Pan Set",
        "Digital Fitness Tracker",
        "Memory Foam Pillow",
        "Smart Home Security Camera",
        "Foldable Travel Backpack",
        "Electric Coffee Maker",
        "Cotton Bed Sheets Set",
        "Wireless Computer Mouse",
        "Multi-Tool Pocket Knife",
        "LED Desk Lamp with Adjustable Brightness",
        "Glass Food Storage Containers",
        "Canvas Wall Art Prints",
        "Premium Yoga Mat",
        "Insulated Lunch Box",
        "Digital Alarm Clock",
        "Essential Oil Diffuser",
        "Microfiber Cleaning Cloths Set"
    ]

    Transactions["TransDesc"] = [list[random.randint(0, len(list) - 1)] for i in range(transactions)]

    print(Transactions)

    Transactions.to_csv("TransactionsTest.csv", index=False)






if __name__ == '__main__':
    customerGeneration()
    transactionGeneration()
    print("Hi")