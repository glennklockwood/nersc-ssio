.PHONY: clean all
OBJS=llapi-perf test-getstripe test-getobdcount test-record-malloc

LDFLAGS=-llustreapi -dynamic

all: $(OBJS)

clean:
	@-rm -v $(OBJS)
