Team Members:
=============

Michel Khalaf  
Christopher Habib-Rahme  
Marianne Jbeily    
Mariella El Jreidy 

# Kafka multi-broker producer-consumer: 

The aim of this application is to fetch data relative to a particular hashtag from twitter using Twitter-API and sends it to consumers using a Kafka multi-broker producer-consumer application with a customized producer-partition strategy.

# Demo:

https://drive.google.com/file/d/1llJqTgEsHCP0TSHJFYWia2A7QhhLG_Gg/view?usp=sharing

# Executing the code:

1- starting zookeeper server:
	- open a terminal at the kafka folder
	- run the following command: bin\windows\zookeeper-server-start.bat config\zookeeper.properties

2- starting kafka server:
	- open a terminal at the kafka folder
	- run the following command: bin\windows\kafka-server-start.bat config\server.properties


3- we expand our cluster to three nodes, we have to make a config file for each broker with the following commands:
	- copy config\server.properties config\server1.properties
	- copy config\server.properties config\server2.properties

4- edit these files to the following properties:
config/server-1.properties:
    broker.id=1
    listeners=PLAINTEXT://:9093
    log.dirs=/tmp/kafka-logs-1

config/server-2.properties:
    broker.id=2
    listeners=PLAINTEXT://:9094
    log.dirs=/tmp/kafka-logs-2

5- we start the two new brokers in two terminals (just like step 2) with the commands:
	- bin\windows\kafka-server-start.bat config\server1.properties
	- bin\windows\kafka-server-start.bat config\server2.properties

6- create my-replicated-twitter topic with partition count of 3 and replication factor of 3:
	- open a terminal at the kafka folder
	- bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 3 --topic my-replicated-twitter


7- we now have three running kafka brokers and we want to see which broker is the leader so we can kill it, type the following command:
	- bin\windows\kafka-topics.bat --describe --bootstrap-server localhost:9092 --topic my-replicated-twitter
we can see the which broker is the leader, we will kill it while executing our code to test our application's reliability. Let's consider the leader is node 2.


8- run three consumers giving each the same groupID as the others as a parameter (groupID can be anything)

9- write the following command on a terminal open in the kafka folder (without executing it for now):
	- wmic process where "caption = 'java.exe' and commandline like '%server-2.properties%'" get processid

10- run the TwitterKafkaProducer providing three hashtags as argument (for example: #USA #BLM #Covid19)

11- run the command on step 9, note the processid n and execute the following command:
	- taskkill /pid n /f
Be careful to run this command while the application is running, before all the tweets have reached the corresponding consumers

12- to check who's the new leader among the brokers, execute the command on step 7 







