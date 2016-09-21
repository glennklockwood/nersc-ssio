.PHONY: clean

CFLAGS=-I/opt/local/include -Wno-deprecated-declarations -g
LDFLAGS=-L/opt/local/lib -Bstatic

all: producer sha1

producer: producer.c
	$(CC) $(CPPFLAGS) -DCONFIG_FILE=\"amqpcreds.conf\" $(CFLAGS) -o $@ $< $(LDFLAGS) -lrabbitmq  -lssl -lcrypto

sha1: sha1.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $< $(LDFLAGS) -lssl -lcrypto

processfile: processfile.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $< $(LDFLAGS) -lssl -lcrypto -lz

clean:
	rm producer *.o sha1
