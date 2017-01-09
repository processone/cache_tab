%%%----------------------------------------------------------------------
%%% File    : cache_tab_test.erl
%%% Author  : Mickael Remond <mremond@process-one.net>
%%% Purpose : cache_tab module tests 
%%% Created : 10 Dec 2015 by Mickael Remond <mremond@process-one.net>
%%%
%%%
%%% Copyright (C) 2002-2017 ProcessOne, SARL. All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%%%----------------------------------------------------------------------

-module(cache_tab_test).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

get_proc_num() ->
    case catch erlang:system_info(logical_processors) of
        Num when is_integer(Num) ->
            Num;
        _ ->
            1
    end.

start_test() ->
    ?assertEqual(ok, application:start(cache_tab)).

dirty_insert_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    ?assertEqual(ok, cache_tab:dirty_insert(test_tbl, 1, a, fun() -> ok end)),
    ?assertEqual({ok, a}, cache_tab:dirty_lookup(test_tbl, 1, fun() -> error end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

dirty_delete_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    ?assertEqual(ok, cache_tab:dirty_insert(test_tbl, 1, a, fun() -> ok end)),
    ?assertEqual(ok, cache_tab:dirty_delete(test_tbl, 1, fun() -> ok end)),
    ?assertEqual(error, cache_tab:dirty_lookup(test_tbl, 1, fun() -> error end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

insert_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    ?assertEqual(ok, cache_tab:insert(test_tbl, 1, a, fun() -> ok end)),
    ?assertEqual({ok, a}, cache_tab:lookup(test_tbl, 1, fun() -> error end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

double_insert_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    ?assertEqual(ok, cache_tab:insert(test_tbl, 1, a, fun() -> ok end)),
    ?assertEqual({ok, a}, cache_tab:lookup(test_tbl, 1, fun() -> error end)),
    ?assertEqual(ok, cache_tab:insert(test_tbl, 1, a, fun() -> ok end)),
    ?assertEqual({ok, a}, cache_tab:lookup(test_tbl, 1, fun() -> error end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

insert_bad_fun_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [{warn, false}])),
    ?assertEqual(ok, cache_tab:insert(test_tbl, 1, a,
				      fun() -> erlang:error(test_error) end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

delete_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    ?assertEqual(ok, cache_tab:insert(test_tbl, 1, a, fun() -> ok end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl, 1, fun() -> ok end)),
    ?assertEqual(error, cache_tab:lookup(test_tbl, 1, fun() -> error end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

delete_bad_fun_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [{warn, false}])),
    ?assertEqual(ok, cache_tab:delete(test_tbl, 1, fun() -> erlang:error(test_error) end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

clean_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    ?assertEqual(ok, cache_tab:dirty_insert(test_tbl, 1, a, fun() -> ok end)),
    {ok, 1} = cache_tab:info(test_tbl, size),
    ?assertEqual(ok, cache_tab:clean(test_tbl)),
    {ok, 0} = cache_tab:info(test_tbl, size),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

dirty_lookup_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    ?assertEqual(
       {ok, a},
       cache_tab:dirty_lookup(test_tbl, 1, fun() -> {ok, a} end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

lookup_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    ?assertEqual(
       {ok, a},
       cache_tab:lookup(test_tbl, 1, fun() -> {ok, a} end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

lookup_bad_fun_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [{warn, false}])),
    ?assertEqual(
       error,
       cache_tab:lookup(test_tbl, 1, fun() -> erlang:error(test_error) end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

tab2list_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    KeyVals = lists:zip(lists:seq(1, 10), lists:seq(1, 10)),
    lists:foreach(
      fun({K, V}) ->
	      ?assertEqual(ok, cache_tab:insert(test_tbl, K, V,
						fun() -> ok end))
      end, KeyVals),
    ?assertEqual(KeyVals, lists:sort(cache_tab:tab2list(test_tbl))),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

all_test() ->
    Tabs = [tab1, tab2, tab3],
    lists:foreach(
      fun(Tab) -> ?assertEqual(ok, cache_tab:new(Tab, [])) end,
      Tabs),
    ?assertEqual(Tabs, cache_tab:all()),
    lists:foreach(
      fun(Tab) -> ?assertEqual(ok, cache_tab:delete(Tab)) end,
      Tabs).

size_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    lists:foreach(
      fun(I) ->
	      ?assertEqual(ok, cache_tab:dirty_insert(test_tbl, I, I,
						      fun() -> ok end))
      end, lists:seq(1, 10)),
    ?assertEqual({ok, 10}, cache_tab:info(test_tbl, size)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

ratio_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    lists:foreach(
      fun(I) ->
	      ?assertEqual(
		 {ok, I},
		 cache_tab:dirty_lookup(test_tbl, I, fun() -> {ok, I} end)),
	      ?assertEqual(
		 {ok, I},
		 cache_tab:dirty_lookup(test_tbl, I, fun() -> {ok, I} end))
      end, lists:seq(1, 10)),
    {ok, Opts} = cache_tab:info(test_tbl, ratio),
    ?assertEqual(10, proplists:get_value(hits, Opts)),
    ?assertEqual(10, proplists:get_value(miss, Opts)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

all_info_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    ?assertMatch({ok, _}, cache_tab:info(test_tbl, all)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

wrong_option_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    ?assertEqual({error, badarg}, cache_tab:info(test_tbl, foo)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

collect_sum(Sum) ->
    receive
	I when is_integer(I) -> collect_sum(Sum + 1)
    after 0 -> Sum
    end.

dirty_cache_missed_enabled_test() ->
    cache_missed_enabled(dirty_lookup).

cache_missed_enabled_test() ->
    cache_missed_enabled(lookup).

cache_missed_enabled(F) ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [])),
    Self = self(),
    lists:foreach(
      fun(_) ->
	      ?assertEqual(
		 error,
		 cache_tab:F(
		   test_tbl, 1, fun() -> Self ! 1 end))
      end, lists:seq(1, 10)),
    ?assertEqual(1, collect_sum(0)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

dirty_cache_missed_disabled_test() ->
    cache_missed_disabled(dirty_lookup).

cache_missed_disabled_test() ->
    cache_missed_disabled(lookup).

cache_missed_disabled(F) ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [{cache_missed, false}])),
    Self = self(),
    lists:foreach(
      fun(_) ->
	      ?assertEqual(
		 error,
		 cache_tab:F(
		   test_tbl, 1, fun() -> Self ! 1 end))
      end, lists:seq(1, 10)),
    ?assertEqual(10, collect_sum(0)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

lifetime_with_lru_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [{life_time, 1}])),
    ?assertEqual(ok, cache_tab:dirty_insert(test_tbl, 1, a, fun() -> ok end)),
    timer:sleep(timer:seconds(2)),
    ?assertEqual({ok, a},
		 cache_tab:dirty_lookup(test_tbl, 1, fun() -> error end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).

lifetime_without_lru_test() ->
    ?assertEqual(ok, cache_tab:new(test_tbl, [{life_time, 1}, {lru, false}])),
    ?assertEqual(ok, cache_tab:dirty_insert(test_tbl, 1, a, fun() -> ok end)),
    timer:sleep(timer:seconds(2)),
    ?assertEqual(error, cache_tab:dirty_lookup(test_tbl, 1, fun() -> error end)),
    ?assertEqual(ok, cache_tab:delete(test_tbl)).
