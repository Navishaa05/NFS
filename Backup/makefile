# Makefile

# Compiler
CC = gcc

# Targets
all: n s c

# Rules for each target
n: ns.c
	$(CC) ns.c common.c -o n

s: ss.c
	$(CC) ss.c common.c -o s -lmpv

c: client.c
	$(CC) client.c -o c -lmpv

# Clean up generated files
clean:
	rm -f n s c


