%%%----------------------------------------------------------------------
%%% File    : ets_cache_test.erl
%%% Purpose : ets_cache module tests (counters NIF)
%%%
%%% Copyright (C) 2002-2026 ProcessOne, SARL. All Rights Reserved.
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

-module(ets_cache_test).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

start_test() ->
    ?assertMatch({ok, _}, application:ensure_all_started(cache_tab)).

new_counter_test() ->
    {ok, C} = ets_cache:new_counter_nif(),
    ?assertEqual(0, ets_cache:get_counter_nif(C)),
    ?assertEqual(ok, ets_cache:delete_counter_nif(C)).

incr_counter_test() ->
    {ok, C} = ets_cache:new_counter_nif(),
    lists:foreach(
      fun(I) -> ?assertEqual(I, ets_cache:incr_counter_nif(C)) end,
      lists:seq(1, 10)),
    ?assertEqual(10, ets_cache:get_counter_nif(C)),
    ?assertEqual(ok, ets_cache:delete_counter_nif(C)).

delete_counter_test() ->
    {ok, C} = ets_cache:new_counter_nif(),
    ?assertEqual(ok, ets_cache:delete_counter_nif(C)),
    ?assertError(badarg, ets_cache:get_counter_nif(C)),
    ?assertError(badarg, ets_cache:incr_counter_nif(C)),
    %% Deleting an already-freed counter is idempotent.
    ?assertEqual(ok, ets_cache:delete_counter_nif(C)).

independent_counters_test() ->
    {ok, C1} = ets_cache:new_counter_nif(),
    {ok, C2} = ets_cache:new_counter_nif(),
    ?assert(C1 =/= C2),
    ?assertEqual(1, ets_cache:incr_counter_nif(C1)),
    ?assertEqual(0, ets_cache:get_counter_nif(C2)),
    ?assertEqual(ok, ets_cache:delete_counter_nif(C1)),
    ?assertEqual(ok, ets_cache:delete_counter_nif(C2)).

reuse_deleted_counter_test() ->
    {ok, C1} = ets_cache:new_counter_nif(),
    ?assertEqual(ok, ets_cache:delete_counter_nif(C1)),
    {ok, C2} = ets_cache:new_counter_nif(),
    ?assertEqual(0, ets_cache:get_counter_nif(C2)),
    ?assertEqual(ok, ets_cache:delete_counter_nif(C2)).

get_counter_out_of_range_test() ->
    ?assertError(badarg, ets_cache:get_counter_nif(1000000)).

delete_counter_out_of_range_test() ->
    ?assertError(badarg, ets_cache:delete_counter_nif(1000000)).

system_limit_and_recovery_test() ->
    %% Drain every remaining counter slot, whatever the actual maximum is
    %% (avoids assuming a clean global counter table or duplicating the
    %% default from ets_cache.erl).
    {Counters, Limit} = drain_counters([]),
    ?assertMatch({error, {system_limit, _}}, Limit),
    {error, {system_limit, Max}} = Limit,
    %% Freeing a single slot must make room for exactly one new counter.
    [First|Rest] = Counters,
    ?assertEqual(ok, ets_cache:delete_counter_nif(First)),
    {ok, Reused} = ets_cache:new_counter_nif(),
    ?assertEqual(0, ets_cache:get_counter_nif(Reused)),
    ?assertEqual({error, {system_limit, Max}}, ets_cache:new_counter_nif()),
    lists:foreach(fun ets_cache:delete_counter_nif/1, [Reused|Rest]).

drain_counters(Acc) ->
    case ets_cache:new_counter_nif() of
	{ok, C} -> drain_counters([C|Acc]);
	Error -> {Acc, Error}
    end.
