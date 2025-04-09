all: broker producer consumer

broker: src/broker.c
	gcc src/broker.c -o broker -lpthread

producer: src/producer.c
	gcc src/producer.c -o producer

consumer: src/consumer.c
	gcc src/consumer.c -o consumer

clean:
	rm -f broker producer consumer
