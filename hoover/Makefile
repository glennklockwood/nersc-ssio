.PHONY: clean

CFLAGS=-I/opt/local/include -Wno-deprecated-declarations -DCONFIG_FILE=\"amqpcreds.conf\" -g
LDFLAGS=-L/opt/local/lib -lrabbitmq -Bstatic

sendfile-dmj: sendfile-dmj.o

clean:
	rm dmj-sendfile *.o
