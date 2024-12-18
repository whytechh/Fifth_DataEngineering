from pymongo import MongoClient, DESCENDING, ASCENDING
import pandas as pd
import json


def connect_to_database():
    client = MongoClient()
    database = client['lab_5']
    collection = database.jobs
    return collection


def read_csv():
    data = pd.read_csv('./tasks/1/task_1_item.csv', encoding='utf-8')
    return data


def sorted_salary(collection):
    result = list(collection.find({}, {'_id': 0})
                            .sort('salary', DESCENDING)
                            .limit(10))
    return result


def filtered_age(collection):
    result = list(collection.find({
                                'age': {'$lt': 30}},
                                {'_id': 0})
                            .sort('salary', DESCENDING)
                            .limit(15))
    return result


def filters(collection):
    result = list(collection.find({
                                'city': 'Лас-Росас',
                                'job': {'$in': ['Менеджер', 'Инженер', 'IT-специалист']}},
                                {'_id': 0})
                            .sort('age', ASCENDING)
                            .limit(10))
    return result


def range_filters(collection):
    result = (collection.count_documents({
                                'age': {'$gt': 30, '$lt': 50},
                                'year': {'$gte': 2019, '$lte': 2022},
                                '$or': [
                                    {'salary': {'$gt': 50000, '$lte': 75000}},
                                    {'salary': {'$gt': 125000, '$lt': 150000}}
                                ]}))
    return result


def save_queries(filename, result):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent=1)


collection = connect_to_database()

data = read_csv()
data_dict = data.to_dict(orient='records')

#collection.insert_many(data_dict)

save_queries('./results/1/sorted_salary.json', sorted_salary(collection))
save_queries('./results/1/filtered_age.json', filtered_age(collection))
save_queries('./results/1/filters.json', filters(collection))
save_queries('./results/1/range_filters.json', {'count': range_filters(collection)})
