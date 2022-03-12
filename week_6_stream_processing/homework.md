Which option allows Kafka to scale
partitions

Which option provide fault tolerance to kafka
replication

What is a compact topic?
Topic which deletes messages after 7 days
Topic which compact messages based on value
Topic which compact messages based on Key **
All topics are compact topic


Role of schemas in Kafka?
Making consumer producer independent of each other
Provide possibility to update messages without breaking change **
Allow control when producing messages **
Share message information with consumers


Which configuration should a producer set to provide guarantee that a message is never lost?
ack=0


From where all can a consumer start consuming messages from?
Beginning in topic **
Latest in topic **
From a particular offset **
From a particular timestamp


What key structure is seen when producing messages via 10 minutes Tumbling window in Kafka stream?
[Key, Start Timestamp, Start Timestamp + 10 mins]


Benefit of Global KTable?
Efficient joins

When joining KTable with KTable partitions should be
Same

When joining KStream with Global KTable partitions should be
Does not matter (can be same or different)


Create two streams with same key and provide a solution where you are joining the two keys