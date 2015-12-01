# In-memory cache application for Erlang / Elixir apps

[![Build Status](https://travis-ci.org/processone/cache_tab.svg?branch=master)](https://travis-ci.org/processone/cache_tab) [![Coverage Status](https://coveralls.io/repos/processone/cache_tab/badge.svg?branch=master&service=github)](https://coveralls.io/github/processone/cache_tab?branch=master) [![Hex version](https://img.shields.io/hexpm/v/cache_tab.svg "Hex version")](https://hex.pm/packages/cache_tab)

`cache_tab` application is intended to proxy back-end operations for
Key-Value insert, lookup and delete and maintain a cache of those
Key-Values in-memory, to save back-end operations.

Operations are intended to be atomic between back-end and cache
tables.

The lifetime of the cache object and the max size of the cache can be
defined as table parameters to limit the size of the in-memory tables.

## Building

`cache_tab` application can be build as follow:

    make

It is a rebar-compatible OTP application. Alternatively, you can build
it with rebar:

    rebar get-deps compile

## Usage

You can start the application with the command:

```
$ erl -pa ebin/
Erlang/OTP 18 [erts-7.1] [source] [64-bit] [smp:4:4] [async-threads:10] [hipe] [kernel-poll:false] [dtrace]

Eshell V7.1  (abort with ^G)
1> application:start(cache_tab).
```

Then, you can create a table for a specific type of data to cache. You
can create several tables to separate your cached data. The following
command will create a table named `tab_name`:

```
cache_tab:new(tab_name, [{life_time, Seconds}, {max_size, N}]).
```

The optional `life_time` option is used to define cache entry
expiration. The optional `max_size` option is used to limit number of
entries to add in cache.

You can insert data into the cache with:

```
cache_tab:insert(tab_name, <<"key">>, <<"value">>, fun() -> do_something() end).
```

Cache inserts are designed to be able to act as a proxy to backend
insert. That's the purpose of the fun called as last parameter
(`do_something`). Return of the fun is ignored, but if it crashes,
cache will not be stored.  If value did not change compared to
previous insert, the fun will not be called, saving possibly unneeded
write operations

You can also lookup data from the cache:

```
cache_tab:lookup(tab_name, <<"key">>, fun() -> read_value_if_not_cached() end).
```

The fun in last parameter is used to read and cache the value from
your back-end if it is not already in cache.

It is expected to return either `{ok, Value}` or `error`.

You can delete entries from back-end and cache with command:


```
cache_tab:delete(tab_name, <<"key">>, fun() -> do_something() end),
```

Info command will report about the hits / misses to help you tune your
cache size / lifetime parameters:

```
cache_tab:info(tab_name, Info).
```

Info parameter can be: `size`, `ratio` or `all`.

## Development

### Test

#### Unit test

You can run eunit test with the command:

    $ make test
