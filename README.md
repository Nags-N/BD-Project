# For Mac:

Step 1: Start Zookeeper

cd /Users/nandakishorep/Downloads/kafka_2.12-3.9.0
bin/zookeeper-server-start.sh config/zookeeper.properties

Step 2: Start Kafka Broker

cd /Users/nandakishorep/Downloads/kafka_2.12-3.9.0
bin/kafka-server-start.sh config/server.properties

Step 3: Run Flask Application

cd /Users/nandakishorep/BD Project
python3.10 app.py

Step 4: Run Spark Job

cd /Users/nandakishorep/BD Project
spark-submit --master "local[*]" spark_job.py

Step 5: Run Publisher

cd /Users/nandakishorep/BD Project
python3.10 main_publisher.py

Step 6: Run Cluster Publisher

cd /Users/nandakishorep/BD Project
python3.10 cluster_publisher.py

Step 7: Run Subscriber

cd /Users/nandakishorep/BD Project
python3.10 subscriber.py

Step 8: Run Load Testing/Simulated Client

cd /Users/nandakishorep/BD Project
python3.10 load_testing.py

Step 9: View Kafka Console Output

cd /Users/nandakishorep/Downloads/kafka_2.12-3.9.0
bin/kafka-console-consumer.sh --topic emoji-output --from-beginning --bootstrap-server localhost:9092
