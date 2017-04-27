%%%-------------------------------------------------------------------
%%% @author Evgeny Khramtsov <ekhramtsov@process-one.net>
%%% Created : 24 Apr 2017 by Evgeny Khramtsov <ekhramtsov@process-one.net>
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
%%%-------------------------------------------------------------------
-module(ets_cache_options).

%% API
-export([max_size/1, life_time/1, cache_missed/1, counter/1]).

%%%===================================================================
%%% API
%%%===================================================================
-spec max_size(atom()) -> {ok, pos_integer() | infinity}.
max_size(_) -> {ok, infinity}.

-spec life_time(atom()) -> {ok, pos_integer() | infinity}.
life_time(_) -> {ok, infinity}.

-spec cache_missed(atom()) -> {ok, boolean()}.
cache_missed(_) -> {ok, true}.

-spec counter(non_neg_integer()) -> {ok, non_neg_integer()} | undefined.
counter(_) -> undefined.

%%%===================================================================
%%% Internal functions
%%%===================================================================
