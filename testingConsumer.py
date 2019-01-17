# -*- coding: utf-8 -*-
"""
Created on Fri Dec  7 14:08:27 2018

@author: ravi_
"""

from kafka import KafkaConsumer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch
import json
from _collections import defaultdict

assignment = "HW3"
kbIndex = "sentiment"
topic = 'twitter'

consumer = KafkaConsumer(topic, value_deserializer=lambda m: json.loads(m.decode('utf-8')), bootstrap_servers='localhost:9092')
print(consumer.fetch_messages)
es = Elasticsearch()

if not es.indices.exists(kbIndex):  # create if the index does not exist
    es.indices.create(kbIndex)


mapping = {
    assignment: {
        property: {
                "author":{"type": "string"},
                "sentiment":{"type": "string"},
                "tagType":{"type": "integer"},
                # "polarity":{"type": "string"},
                "date":{"type": "date", "format": "EEE MMM dd HH:mm:ss Z yyyy"},
                "message":{"type": "string", "index": "not_analyzed"},
                "hashtags":{"type": "string", "index": "not_analyzed"},
                "location":{"type": "geo_point", "index": "not_analyzed"},
        }
    }
}

tagTypes = 0
def main():
    analysisdata = SentimentIntensityAnalyzer()
    for msg in consumer:
        #print(msg))
        tweetdata = json.loads(json.dumps(msg.value))
        #print(tweetdata)
        tweettext = tweetdata.get("text")
        
        if tweetdata.get("place"):
            location = tweetdata.get("place").get("bounding_box").get("coordinates")
            #print ("Geo:Latitude: ", location)
            print("Tweet: ", tweettext)
            scorenumber = analysisdata.polarity_scores(tweettext)
            print("Printing the score number.......................")
            print(scorenumber)
            if scorenumber["compound"]<0:
                sentiment="negative"
                print(sentiment)
            elif scorenumber["compound"] == 0:
                sentiment = "neutral"
                print(sentiment)
            else:
                sentiment = "positive"
                print(sentiment)

        tLeft = tweetdata.get("place").get("bounding_box").get("coordinates")[0][0]
        tRight = tweetdata.get("place").get("bounding_box").get("coordinates")[0][2]
        bRight = tweetdata.get("place").get("bounding_box").get("coordinates")[0][1]
        bLeft = tweetdata.get("place").get("bounding_box").get("coordinates")[0][3]
        loc = dict()
        loc["top"] = tLeft
        loc["left"] = bLeft
        loc["bottom"] = bRight
        loc["right"] = tRight
        #print ("elasticSearch: ",loc)

    if "obama" in tweettext:
        tagType = 1
    elif "trump" in tweettext:
        tagType = 2
    content = {"author": tweetdata["user"]["screen_name"],
               "sentiment": sentiment,
               "polarity":scorenumber,
               "date": tweetdata["created_at"],
               "message": tweettext,
               "location": tLeft,
               "tagType": tagType
               }
    es.indices.put_mapping(index=kbIndex, doc_type=assignment, body=mapping)
    es.index(index=kbIndex, doc_type=assignment, body=content)

    es.indices.refresh(index="sentiment")

    if __name__ == "_main_":
        print("Starting point")
        consumer_key = "LIMEPbtSryyvDb8rvLEC4FHdB"
        consumer_secret = "8rBSedJ98CECie5u10kOOVZOlvlC8mDnbtQvr0ikuUkznQQwZ2"
        access_token = "792221060338823168-6LwksBUOaWrHRbN6if0bbitfPzr34IJ"
        access_secret = "ZXXhdOX3IWiAYARtbv2lXyO3nLFCozkIqcwjVxX9pMatt"

main()