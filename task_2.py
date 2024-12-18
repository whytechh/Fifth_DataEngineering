import pickle
import json
from pymongo import MongoClient, ASCENDING, DESCENDING


def connect_to_database():
    client = MongoClient()
    database = client['lab_5']
    collection = database.jobs
    return collection


def read_pickle():
    with open(r'lab_5\tasks\2\task_2_item.pkl', 'rb') as file:
        return pickle.load(file)


def salary_stats(collection):
    query = [
        {
            '$group': 
            {
                '_id': 'result',
                'max_salary': {'$max': '$salary'},
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def job_stats(collection):
    query = [
        {
            '$group':
            {
                '_id': '$job',
                'count': {'$sum': 1}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def salary_by_city(collection):
    query = [
        {
            '$group':
            {
                '_id': '$city',
                'max_salary': {'$max': '$salary'},
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def salary_by_job(collection):
    query = [
        {
            '$group':
            {
                '_id': '$job',
                'max_salary': {'$max': '$salary'},
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def age_by_city(collection):
    query = [
        {
            '$group':
            {
                '_id': '$city',
                'max_age': {'$max': '$age'},
                'min_age': {'$min': '$age'},
                'avg_age': {'$avg': '$age'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def age_by_job(collection):
    query = [
        {
            '$group':
            {
                '_id': '$job',
                'max_age': {'$max': '$age'},
                'min_age': {'$min': '$age'},
                'avg_age': {'$avg': '$age'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def maxSalary_by_minAge(collection):
    query = [
        {
            '$group':
            {
                '_id': '$age',
                'max_salary': {'$max': '$salary'}
            }
        },
        {
            '$sort':
            {
                '_id': ASCENDING
            }
        },
        {
            '$limit': 1
        }
    ]
    return(list(collection.aggregate(query)))


def minSalary_by_maxAge(collection):
    query = [
        {
            '$group':
            {
                '_id': '$age',
                'min_salary': {'$min': '$salary'}
            }
        },
        {
            '$sort':
            {
                '_id': DESCENDING
            }
        },
        {
            '$limit': 1
        }
    ]
    return(list(collection.aggregate(query)))


def statsAge_byCity_withFilteredSalary(collection):
    query = [
        {
            '$match':
            {
                'salary': {'$gt': 50000}
            }
        },
        {
            '$group':
            {
                '_id': '$city',
                'max_age': {'$max': '$age'},
                'min_age': {'$min': '$age'},
                'avg_age': {'$avg': '$age'}
            }
        },
        {
            '$sort':
            {
                'avg_age': DESCENDING
            }
        }
    ]
    return(list(collection.aggregate(query)))


def statsSalary_withRangeFilters(collection):
    query = [
        {
            '$match':
            {
                'city': {'$in': ['Трухильо', 'Загреб', 'Фигерас', 'Бильбао']},
                'job': {'$in': ['IT-специалист', 'Психолог', 'Косметолог']},
                '$or': [
                    {'age': {'$gt': 18, '$lt': 25}},
                    {'age': {'$gt': 50, '$lt': 65}}
                ]
            }
        },
        {
            '$group':
            {
                '_id': 'result',
                'max_salary': {'$max': '$salary'},
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'}
            }
        }
    ]
    return(list(collection.aggregate(query))), 


def custom_query(collection):
    query = [
        {
            '$match':
            {
                'job': 'Косметолог',
                '$or': [
                    {'year': {'$gte': 2005, '$lte': 2010}},
                    {'year': {'$gte': 2010, '$lte': 2015}}
                ]
            }
        },
        {
            '$group':
            {
                '_id': '$year',
                'max_age': {'$max': '$age'},
                'min_age': {'$min': '$age'},
                'avg_age': {'$avg': '$age'},
                'avg_salary': {'$avg': '$salary'}
                
            }
        },
        {
            '$sort':
            {
                '_id': DESCENDING
            }
        }
    ]
    return(list(collection.aggregate(query)))


def save_queries(filename, result):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent=1)


collection = connect_to_database()

data = read_pickle()

#collection.insert_many(data)

save_queries(r'lab_5\results\2\salary_stats.json', salary_stats(collection))
save_queries(r'lab_5\results\2\job_stats.json', job_stats(collection))
save_queries(r'lab_5\results\2\salary_by_city.json', salary_by_city(collection))
save_queries(r'lab_5\results\2\salary_by_job.json', salary_by_job(collection))
save_queries(r'lab_5\results\2\age_by_city.json', age_by_city(collection))
save_queries(r'lab_5\results\2\age_by_job.json', age_by_job(collection))
save_queries(r'lab_5\results\2\maxSalary_by_minAge.json', maxSalary_by_minAge(collection))
save_queries(r'lab_5\results\2\minSalary_by_maxAge.json', minSalary_by_maxAge(collection))
save_queries(r'lab_5\results\2\statsAge_byCity_withFilteredSalary.json', statsAge_byCity_withFilteredSalary(collection))
save_queries(r'lab_5\results\2\statsSalary_withRangeFilters.json', statsSalary_withRangeFilters(collection))
save_queries(r'lab_5\results\2\custom_query.json', custom_query(collection))