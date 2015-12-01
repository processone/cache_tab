all: src

src:
	rebar get-deps compile

clean:
	rebar clean

.PHONY: clean src all
