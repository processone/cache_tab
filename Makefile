all: src

src:
	rebar get-deps compile

clean:
	rebar clean

test: all
	rebar -v skip_deps=true eunit

.PHONY: clean src all
