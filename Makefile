.PHONY: all
all: src
	CC=clang CXX=clang++ cargo build --release


.PHONY: no-throttle
no-throttle: src
	CC=clang CXX=clang++ cargo build --release --no-default-features
