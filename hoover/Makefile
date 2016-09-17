.PHONY: clean

CFLAGS=-I/opt/local/include -Wno-deprecated-declarations -DCONFIG_FILE=\"amqpcreds.conf\" -g
LDFLAGS=-L/opt/local/lib -lrabbitmq -Bstatic

producer: producer.o

clean:
	rm producer *.o
