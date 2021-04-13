INCLUDE = -I /Library/PostgreSQL/13/include/postgresql/server
CFLAGS = -bundle -flat_namespace -undefined suppress

%.o:	%.c
	$(CC) $(INCLUDE) -c  $< -o $@
%.so:	%.o
	$(CC) $(CFLAGS) -o $@ $<

all:	tildeop.so
clean:
	$(RM) *.o *.so