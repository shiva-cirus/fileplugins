Usecase #3 :

Demo Streaming Capability of CDAP. The Pipleline will read a message from Kakfa topic and
persist this to google BigQuery table. 

Setup
1. Setup Kafka Broker. 
2. Create Kafka topic. 
eg: kafka-console-producer --broker-list localhost:9092 --topic cdaptest

3. Create a GCP BigQuery Data set and table with the below schema. 

				    |- _id: string                                                                             
                    |- guid: string                                                                                               
                    |- age: integer                                                                                               
                    |- isActive: boolean                                                                                          
                    |- balance: string                                                                                            
                    |- picture: string                                                                                            
                    |- index: integer                                                                                             
                    |- first: string                                                                                              
                    |- last: string                                                                                               
                    |- company: string

4. Create a service account that has rights to insert and query records from this table. 

5. Import pipeline using file CDAPPOV-UC3-cdap-data-streams.json. 

6. Provide the required properties Note: Due to streaming on Spark MACROS are not supported. 

Once required values are provided for pipeline then deploy adn run .

7. Publish sample data to Kafka cluster. 
kafka-console-producer --broker-list localhost:9092 --topic cdaptest < test.csv

8. Check data in google big query.

eg:
Shivakumars-MacBook-Pro-2:bin shiva$ bq query --nouse_legacy_sql 'SELECT * FROM `midyear-courage-256620`.CDAPPOV.customer LIMIT 1000'
Waiting on bqjob_r2513afb11cdc65eb_0000016e4c0d8586_1 ... (0s) Current status: DONE
+-------------+---------------+-----+----------+---------+-----------------+-------+-------+-------+------------------+
|     _id     |     guid      | age | isActive | balance |     picture     | index | first | last  |     company      |
+-------------+---------------+-----+----------+---------+-----------------+-------+-------+-------+------------------+
| 1           | wer-sfg-were  |  10 |     true | blnc    | pic             |     1 | f     | l     | c                |
| id-02020302 | as7s-923-asdd |  10 |    false | 300     | http://photo.id |     1 | Tom   | Smith | StartupAnalytics |
+-------------+---------------+-----+----------+---------+-----------------+-------+-------+-------+------------------+