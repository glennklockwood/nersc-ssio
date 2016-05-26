.PHONY: clean all
OBJS=llapi-perf test-getstripe test-getobdcount test-record-malloc

CFLAGS=-O0 -g
LDFLAGS=-llustreapi -dynamic

all: $(OBJS)

clean:
	@-rm -v $(OBJS)
