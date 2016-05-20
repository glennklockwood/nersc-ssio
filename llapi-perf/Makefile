.PHONY: clean all
OBJS=llapi-perf test-getstripe test-getobdcount

LDFLAGS=-llustreapi -dynamic

all: $(OBJS)

clean:
	@-rm -v $(OBJS)
