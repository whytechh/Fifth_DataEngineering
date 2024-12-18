import pandas as pd
import json
from pymongo import MongoClient, DESCENDING, ASCENDING


def connect_to_database():
    client = MongoClient()
    database = client['lab_5']
    collection = database.audio_features
    return collection


def read_csv():
    data = pd.read_csv(r'lab_5\tasks\4\bts_p1.csv', encoding='utf-8')
    column_drop = ['Unnamed: 0']
    data = data.drop(columns=column_drop)
    data['Release'] = pd.to_datetime(data['Release']).dt.strftime('%Y-%m-%d')
    return data


def read_json():
    data = pd.read_json(r'lab_5\tasks\4\bts_p2.json', encoding='utf-8')
    column_drop = ['Unnamed: 0']
    data = data.drop(columns=column_drop)
    data['Release'] = pd.to_datetime(data['Release']).dt.strftime('%Y-%m-%d')
    return data


def sorted_instrumentalness(collection):
    result = list(collection.find({}, {'_id': 0})
                            .sort('instrumentalness', DESCENDING)
                            .limit(3))
    return result


def filtered_energy(collection):
    result = list(collection.find({
                                'energy': {'$gt': 0.5}},
                                {'_id': 0})
                            .sort('duration_ms', ASCENDING)
                            .limit(10))
    return result


def filters(collection):
    result = list(collection.find({
                                'Artist': 'Coldplay, BTS',
                                'mode': 0},
                                {'_id': 0})
                            .sort('Title', ASCENDING))
    return result


def range_filters(collection):
    result = (collection.count_documents({
                                'liveness': {'$gt': 0.4, '$lt': 0.6},
                                'energy': {'$gte': 0.5, '$lte': 1},
                                '$or': [
                                    {'Release': {'$gt': '2013-01-01', '$lte': '2015-01-01'}},
                                    {'Release': {'$gt': '2015-01-01', '$lt': '2017-01-01'}}
                                ]}))
    return result


def filtered_tempo(collection):
    result = list(collection.find({
                                'tempo': {'$gt': 90, '$lte': 100}},
                                {'_id': 0}
                                )
                            .sort('Release', ASCENDING))
    return result


def loudness_stats(collection):
    query = [
        {
            '$group': 
            {
                '_id': 'stats_loudness',
                'max_loudness': {'$max': '$loudness'},
                'min_loudness': {'$min': '$loudness'},
                'avg_loudness': {'$avg': '$loudness'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def key_stats(collection):
    query = [
        {
            '$group':
            {
                '_id': '$key',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort':
            {
                '_id': ASCENDING
            }
        }
    ]
    return(list(collection.aggregate(query)))


def acousticness_by_mode(collection):
    query = [
        {
            '$group':
            {
                '_id': '$mode',
                'max_acousticness': {'$max': '$acousticness'},
                'min_acousticness': {'$min': '$acousticness'},
                'avg_acousticness': {'$avg': '$acousticness'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def speechiness_by_key(collection):
    query = [
        {
            '$group':
            {
                '_id': '$key',
                'max_speechiness': {'$max': '$speechiness'},
                'min_speechiness': {'$min': '$speechiness'},
                'avg_speechiness': {'$avg': '$speechiness'}
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


def danceability_by_mode(collection):
    query = [
        {
            '$group':
            {
                '_id': '$mode',
                'max_danceability': {'$max': '$danceability'},
                'min_danceability': {'$min': '$danceability'},
                'avg_danceability': {'$avg': '$danceability'}
            }
        }
    ]
    return(list(collection.aggregate(query)))


def delete_by_danceability(collection):
    return collection.delete_many(
    {
        'danceability' : {'$lt': 0.5, '$gt': 0.3}
    })


def update_liveness(collection):
    return collection.update_many({},
    {
        '$inc':
        {
            'liveness': 0.1
        }
    })


def decrease_valence(collection):
    return collection.update_many(
    {
        'Artist': 'Coldplay, BTS'
    },
    {
        '$mul':
        {
            'valence': 0.95
        }
    })


def complex_decrease(collection):
    return collection.update_many(
    {
        'key': {'$in': [1, 3, 5, 7, 9, 11]},
        '$or': 
        [
            {'duration_ms': {'$gte': 250000}},
            {'duration_ms': {'$lte': 300000}}
        ] 
    },
    {
        '$mul':
        {
            'valence': 0.85,
            'energy': 0.85
        }
    })


def date_delete(collection):
    return collection.delete_many(
    {
        'Release': {'$gte': '2013-01-01', '$lte': '2018-01-01'}
    })


def save_queries(filename, result):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent=1)


collection = connect_to_database()

data_csv = read_csv()
data_csv_dict = data_csv.to_dict(orient='records')

data_json = read_json()
data_json_dict = data_json.to_dict(orient='records')

#collection.insert_many(data_csv_dict)
#collection.insert_many(data_json_dict)

save_queries(r'lab_5\results\4\sorted_instrumentalness.json', sorted_instrumentalness(collection))
save_queries(r'lab_5\results\4\filtered_energy.json', filtered_energy(collection))
save_queries(r'lab_5\results\4\filters.json', filters(collection))
save_queries(r'lab_5\results\4\range_filters.json', {'count': range_filters(collection)})
save_queries(r'lab_5\results\4\filtered_tempo.json', filtered_tempo(collection))
save_queries(r'lab_5\results\4\loudness_stats.json', loudness_stats(collection))
save_queries(r'lab_5\results\4\key_stats.json', key_stats(collection))
save_queries(r'lab_5\results\4\acousticness_by_mode.json', acousticness_by_mode(collection))
save_queries(r'lab_5\results\4\speechiness_by_key.json', speechiness_by_key(collection))
save_queries(r'lab_5\results\4\danceability_by_mode.json', danceability_by_mode(collection))
print(delete_by_danceability(collection))
print(update_liveness(collection))
print(decrease_valence(collection))
print(complex_decrease(collection))
print(date_delete(collection))
