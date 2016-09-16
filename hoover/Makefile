.PHONY: clean

CFLAGS=-I/opt/local/include -Wno-deprecated-declarations -DCONFIG_FILE=\"amqpcreds.conf\"
LDFLAGS=-L/opt/local/lib -lrabbitmq -Bstatic

sendfile-dmj: sendfile-dmj.o

clean:
	rm dmj-sendfile *.o
