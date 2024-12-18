import json
from pymongo import MongoClient, ASCENDING, DESCENDING


def connect_to_database():
    client = MongoClient()
    database = client['lab_5']
    collection = database.jobs
    return collection


def read_json():
    with open(r'lab_5\tasks\3\task_3_item.json', 'r', encoding='utf-8') as file:
        return json.load(file)
    

def delete_by_salary(collection):
    return collection.delete_many(
    {
        '$or':
        [
            {'salary' : {'$lt': 25000}},
            {'salary': {'$gt': 175000}}
        ]
    })


def update_age(collection):
    return collection.update_many({},
    {
        '$inc':
        {
            'age': 1
        }
    })


def increase_salaryForJobs(collection):
    return collection.update_many(
    {
        'job': {'$in': ['Повар', 'Психолог']}
    },
    {
        '$mul':
        {
            'salary': 1.05
        }
    })


def increase_salaryForCities(collection):
    return collection.update_many(
    {
        'city': {'$in': ['Санкт-Петербург', 'Белград']}
    },
    {
        '$mul':
        {
            'salary': 1.07
        }
    })


def complex_increase(collection):
    return collection.update_many(
    {
        'job': {'$in': ['Учитель', 'Бухгалтер']},
        'city': {'$in': ['Любляна', 'Сеговия']},
        '$or': 
        [
            {'age': {'$gte': 25}},
            {'age': {'$lte': 40}}
        ] 
    },
    {
        '$mul':
        {
            'salary': 1.1
        }
    })


def custom_delete(collection):
    return collection.delete_many(
    {
        '$or': 
        [
            {'salary': {'$gte': 120000}},
            {'age': {'$lte': 25}},
        ] 
    })


collection = connect_to_database()

data = read_json()

#collection.insert_many(data)

print(delete_by_salary(collection))
print(update_age(collection))
print(increase_salaryForJobs(collection))
print(increase_salaryForCities(collection))
print(complex_increase(collection))
print(custom_delete(collection))