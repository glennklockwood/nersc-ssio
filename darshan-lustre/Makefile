.PHONY: clean all
OBJS=llapi-perf test-getstripe test-getobdcount

CFLAGS=-O0 -g
LDFLAGS=-llustreapi -dynamic

all: $(OBJS)

clean:
	@-rm -v $(OBJS)
