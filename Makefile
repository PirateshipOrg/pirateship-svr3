.PHONY: all
all: src
	CC=clang CXX=clang++ cargo build --release
