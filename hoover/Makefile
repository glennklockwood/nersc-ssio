.PHONY: clean

CFLAGS=-I/opt/local/include -Wno-deprecated-declarations -g
LDFLAGS=-L/opt/local/lib -Bstatic

OBJECTS=producer sha1 test-hooverio

all: $(OBJECTS)

producer: producer.c hooverio.o
	$(CC) $(CPPFLAGS) -DHOOVER_CONFIG_FILE=\"amqpcreds.conf\" $(CFLAGS) -o $@ $? $(LDFLAGS) -lrabbitmq  -lssl -lcrypto -lz

sha1: sha1.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $< $(LDFLAGS) -lssl -lcrypto

hooverio.o: hooverio.c hooverio.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $<

test-hooverio: test-hooverio.c hooverio.o
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $? $(LDFLAGS) -lssl -lcrypto -lz

clean:
	-rm *.o $(OBJECTS)
	-find ./ -type d -name \*.dSYM -print0 | xargs -0 rm -r
