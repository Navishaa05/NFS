# Makefile

# Compiler
CC = gcc

# Targets
all: n s c

# Rules for each target
n: ns.c
	$(CC) ns.c -o n

s: ss.c
	$(CC) ss.c -o s

c: client.c
	$(CC) client.c -o c

# Clean up generated files
clean:
	rm -f n s c
