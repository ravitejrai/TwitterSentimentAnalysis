# TwitterSentimentAnalysis

# Steps to start the analysis (for windows)

1. start the zookeeper in cmd via

`zkServer`

2. start kafka in cmd via

`.\bin\windows\kafka-server-start.bat .\config\server.properties`

3. create a new topic if needed

`kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test`

4. You will use elastic search for running this program. Install it and run via

`elasticsearch.bat`

5. open the python code in pycharm and run

First run the producer and then the consumer 

for the consumer to run you will need elasticsearch to be up and running
