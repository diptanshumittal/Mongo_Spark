import time
from pyspark.sql import SparkSession
          
q1a_query = [{'$match': {'eventname': 'opened'}}, {'$group': {'_id': {'$substr':['$event_time', 0, 10]}, 'count': {'$sum': 1}}}, {'$sort': {'_id': 1}}]
q1b_query = [{'$match': {'eventname': 'discussed'}}, {'$group': {'_id': {'$substr':['$event_time', 0, 10]}, 'count': {'$sum': 1}}}, {'$sort': {'_id': 1}}]
q2_query = [
    {
        '$match': {
            'eventname': 'discussed'
        }
    }, {
        '$group': {
            '_id': {
                'month': {
                    '$substr': [
                        '$event_time', 5, 2
                    ]
                }, 
                'username': '$username'
            }, 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }, {
        '$group': {
            '_id': '$_id.month', 
            'name': {
                '$first': '$_id.username'
            }, 
            'comment_count': {
                '$first': '$count'
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
]
q3_query = [
    {
        '$addFields': {
            'date': {
                '$dateFromString': {
                    'dateString': {
                        '$substr': [
                            '$event_time', 0, 10
                        ]
                    }, 
                    'format': '%Y-%m-%d'
                }
            }
        }
    }, {
        '$addFields': {
            'weekday': {
                '$multiply': [
                    86400000, {
                        '$subtract': [
                            {
                                '$dayOfWeek': '$date'
                            }, 1
                        ]
                    }
                ]
            }
        }
    }, {
        '$addFields': {
            'start_of_week': {
                '$subtract': [
                    '$date', '$weekday'
                ]
            }
        }
    }, {
        '$match': {
            'eventname': 'discussed'
        }
    }, {
        '$group': {
            '_id': {
                'startofweek': '$start_of_week', 
                'username': '$username'
            }, 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }, {
        '$group': {
            '_id': '$_id.startofweek', 
            'name': {
                '$first': '$_id.username'
            }, 
            'comment_count': {
                '$first': '$count'
            }, 
            'startofweek': {
                '$first': '$_id.startofweek'
            }
        }
    }, {
        '$sort': {
            'startofweek': 1
        }
    }
]
q4_query = [
    {
        '$addFields': {
            'date': {
                '$dateFromString': {
                    'dateString': {
                        '$substr': [
                            '$event_time', 0, 10
                        ]
                    }, 
                    'format': '%Y-%m-%d'
                }
            }
        }
    }, {
        '$addFields': {
            'weekday': {
                '$multiply': [
                    86400000, {
                        '$subtract': [
                            {
                                '$dayOfWeek': '$date'
                            }, 1
                        ]
                    }
                ]
            }
        }
    }, {
        '$addFields': {
            'start_of_week': {
                '$subtract': [
                    '$date', '$weekday'
                ]
            }
        }
    }, {
        '$match': {
            'eventname': 'opened'
        }
    }, {
        '$group': {
            '_id': '$start_of_week', 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
]
q5_query = [
    {
        '$match': {
            'eventname': 'merged', 
            'event_time': {
                '$regex': '2010.*'
            }
        }
    }, {
        '$group': {
            '_id': {
                '$substr': [
                    '$event_time', 5, 2
                ]
            }, 
            'count': {
                '$sum': 1
            }, 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
]
q6_query = [
    {
        '$group': {
            '_id': {
                '$substr': [
                    '$event_time', 0, 10
                ]
            }, 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
]
q7_query = [
    {
        '$match': {
            'eventname': 'opened', 
            'event_time': {
                '$regex': '2011.*'
            }
        }
    }, {
        '$group': {
            '_id': '$username', 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }, {
        '$limit': 1
    }
]   

input_uri = "mongodb://127.0.0.1/tyrdatabase1.try3"
output_uri = "mongodb://127.0.0.1/tyrdatabase1.try3"


my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()

print("\n\n\n-----------\nQuery q1_a   :")
df = my_spark.read.format("mongo").option("pipeline",q1a_query).load()
df.show() 
print("\n\n\n-----------\nQuery q1_b  :")  
df = my_spark.read.format("mongo").option("pipeline",q1b_query).load()
df.show()
print("\n\n\n-----------\nQuery q2   :")
df = my_spark.read.format("mongo").option("pipeline",q2_query).load()
df.show()
print("\n\n\n-----------\nQuery q3   :")
df = my_spark.read.format("mongo").option("pipeline",q3_query).load()
df.show()
print("\n\n\n-----------\nQuery q4   :")
df = my_spark.read.format("mongo").option("pipeline",q4_query).load()
df.show()
print("\n\n\n-----------\nQuery q5   :")
df = my_spark.read.format("mongo").option("pipeline",q5_query).load()
df.show()
print("\n\n\n-----------\nQuery q6   :")
df = my_spark.read.format("mongo").option("pipeline",q6_query).load()
df.show()
print("\n\n\n-----------\nQuery q7   :")
df = my_spark.read.format("mongo").option("pipeline",q7_query).load()
df.show()