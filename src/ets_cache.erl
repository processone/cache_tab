%%%-------------------------------------------------------------------
%%% @author Evgeny Khramtsov <ekhramtsov@process-one.net>
%%% Created : 12 Apr 2017 by Evgeny Khramtsov <ekhramtsov@process-one.net>
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
-module(ets_cache).
-behaviour(gen_server).

%% API
-export([new/1, new/2, start_link/2, delete/1, delete/2, delete/3,
	 lookup/2, lookup/3, insert/3, insert/4, insert_new/3, insert_new/4,
	 update/4, update/5, setopts/2, clear/1, clear/2,
	 clean/1, clean/2, filter/2, fold/3, all/0, info/1, info/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).
%% Counters: for tests *only*, don't use them outside this module
-export([new_counter/0, get_counter/1, incr_counter/1, delete_counter/1,
	 new_counter_nif/0, get_counter_nif/1, incr_counter_nif/1,
	 delete_counter_nif/1]).

-compile(no_native).
-on_load(load_nif/0).

-include_lib("stdlib/include/ms_transform.hrl").

-record(state, {tab                     :: atom(),
		clean_timer             :: reference(),
		counter                 :: counter() | undefined,
		cache_missed = true     :: boolean(),
		life_time    = infinity :: milli_seconds() | infinity,
		max_size     = infinity :: pos_integer() | infinity}).

-define(DEFAULT_MAX_CACHE_TABLES, 1024).
-define(IS_EXPIRED(Time, LifeTime, CurrTime),
	(is_integer(LifeTime) andalso (Time + LifeTime =< CurrTime))).

-type counter() :: non_neg_integer().
-type milli_seconds() :: pos_integer().
-type option() :: {max_size, pos_integer() | infinity} |
		  {life_time, milli_seconds() | infinity} |
		  {cache_missed, boolean()}.
-type read_fun() :: fun(() -> {ok, any()} | error).
-type update_fun() :: fun(() -> ok | {ok, any()} | error).
-type filter_fun() :: fun((any(), {ok, any()} | error) -> boolean()).
-type fold_fun() :: fun((any(), {ok, any()} | error, any()) -> any()).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================
start_link(Name, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE,
			  [Name, Opts], []).

-spec new(atom()) -> ok.
new(Name) ->
    new(Name, []).

-spec new(atom(), [option()]) -> ok.
new(Name, Opts) ->
    Opts1 = check_opts(Opts),
    case whereis(Name) of
	undefined ->
	    Spec = {Name, {?MODULE, start_link, [Name, Opts1]},
		    transient, 5000, worker, [?MODULE]},
	    case supervisor:start_child(cache_tab_sup, Spec) of
		{ok, _Pid} -> ok;
		{error, {already_started, _}} -> setopts(Name, Opts1);
		{error, Err} -> erlang:error(Err)
	    end;
	_ ->
	    setopts(Name, Opts1)
    end.

-spec setopts(atom(), [option()]) -> ok.
setopts(Name, Opts) ->
    Opts1 = check_opts(Opts),
    gen_server:cast(Name, {setopts, Opts1}).

-spec delete(atom()) -> ok.
delete(Name) ->
    supervisor:terminate_child(cache_tab_sup, Name),
    supervisor:delete_child(cache_tab_sup, Name),
    ok.

-spec delete(atom(), any()) -> ok.
delete(Name, Key) ->
    delete(Name, Key, [node()]).

-spec delete(atom(), any(), [atom()]) -> ok.
delete(Name, Key, Nodes) ->
    lists:foreach(
      fun(Node) when Node /= node() ->
	      send({Name, Node}, {delete, Key});
	 (_MyNode) ->
	      ets_delete(Name, Key)
      end, Nodes).

-spec insert(atom(), any(), any()) -> boolean().
insert(Name, Key, Val) ->
    insert(Name, Key, Val, [node()]).

-spec insert(atom(), any(), any(), [node()]) -> boolean().
insert(Name, Key, Val, Nodes) ->
    insert(replace, Name, Key, Val, Nodes).

-spec insert_new(atom(), any(), any()) -> boolean().
insert_new(Name, Key, Val) ->
    insert(Name, Key, Val, [node()]).

-spec insert_new(atom(), any(), any(), [node()]) -> boolean().
insert_new(Name, Key, Val, Nodes) ->
    insert(new, Name, Key, Val, Nodes).

-spec lookup(atom(), any()) -> {ok, any()} | any().
lookup(Name, Key) ->
    lookup(Name, Key, undefined).

-spec lookup(atom(), any(), read_fun() | undefined) -> {ok, any()} | any().
lookup(Name, Key, ReadFun) ->
    try ets:lookup(Name, Key) of
	[{_, Val, _} = Obj] ->
	    delete_if_expired(Name, Obj),
	    Val;
	[] when ReadFun /= undefined ->
	    Ver = get_counter(Name),
	    Val = case ReadFun() of
		      {ok, Val1} -> {ok, Val1};
		      error -> error;
		      {error, notfound} -> error;
		      Other -> {error, Other}
		  end,
	    case Val of
		{error, Reason} ->
		    Reason;
		_ ->
		    case get_counter(Name) of
			Ver ->
			    do_insert(new, Name, {Key, Val, current_time()});
			_ ->
			    ok
		    end,
		    Val
	    end;
	[] ->
	    error
    catch _:badarg when ReadFun /= undefined ->
	    ReadFun();
	  _:badarg ->
	    error
    end.

-spec update(atom(), any(), {ok, any()} | error, update_fun()) -> ok | any().
update(Name, Key, Val, UpdateFun) ->
    update(Name, Key, Val, UpdateFun, [node()]).

-spec update(atom(), any(), {ok, any()} | error, update_fun(), [node()]) -> ok | any().
update(Name, Key, Val, UpdateFun, Nodes) ->
    NeedUpdate = try ets:lookup(Name, Key) of
		     [{_, Val, _} = Obj] ->
			 delete_if_expired(Name, Obj);
		     _ ->
			 true
		 catch _:badarg ->
			 true
		 end,
    if NeedUpdate ->
	    NewVal = case UpdateFun() of
			 ok -> Val;
			 {ok, Val1} -> {ok, Val1};
			 error -> error;
			 {error, notfound} -> error;
			 Other -> {error, Other}
		     end,
	    case NewVal of
		{error, Reason} ->
		    Reason;
		_ ->
		    lists:foreach(
		      fun(Node) when Node /= node() ->
			      send({Name, Node}, {delete, Key});
			 (_) ->
			      do_insert(replace, Name, {Key, NewVal, current_time()})
		      end, Nodes),
		    NewVal
	    end;
       true ->
	    Val
    end.

-spec clear(atom()) -> ok.
clear(Name) ->
    clear(Name, [node()]).

-spec clear(atom(), [atom()]) -> ok.
clear(Name, Nodes) ->
    lists:foreach(
      fun(Node) when Node /= node() ->
	      send({Name, Node}, clear);
	 (_MyNode) ->
	      ets_delete_all_objects(Name)
      end, Nodes).

-spec clean(atom()) -> ok.
clean(Name) ->
    clean(Name, [node()]).

-spec clean(atom(), [node()]) -> ok.
clean(Name, Nodes) ->
    lists:foreach(
      fun(Node) ->
	      send({Name, Node}, clean)
      end, Nodes).

-spec filter(atom(), filter_fun()) -> non_neg_integer().
filter(Name, FilterFun) ->
    case whereis(Name) of
	undefined -> 0;
	_ ->
	    {ok, LifeTime} = ets_cache_options:life_time(Name),
	    CurrTime = current_time(),
	    ets_safe_fixtable(Name, true),
	    Num = do_filter(Name, FilterFun, LifeTime, CurrTime,
			    0, ets_first(Name)),
	    ets_safe_fixtable(Name, false),
	    Num
    end.

-spec fold(fold_fun(), any(), atom()) -> any().
fold(FoldFun, Acc, Name) ->
    case whereis(Name) of
	undefined -> Acc;
	_ ->
	    {ok, LifeTime} = ets_cache_options:life_time(Name),
	    CurrTime = current_time(),
	    ets_safe_fixtable(Name, true),
	    NewAcc = do_fold(Name, FoldFun, LifeTime, CurrTime,
			     Acc, ets_first(Name)),
	    ets_safe_fixtable(Name, false),
	    NewAcc
    end.

-spec all() -> [atom()].
all() ->
    [Child || {Child, _, _, [?MODULE]} <- supervisor:which_children(cache_tab_sup)].

-spec info(atom()) -> [proplists:property()].
info(Name) ->
    case ets:info(Name) of
	undefined ->
	    [];
	ETSInfo ->
	    {ok, MaxSize} = ets_cache_options:max_size(Name),
	    {ok, CacheMissed} = ets_cache_options:cache_missed(Name),
	    {ok, LifeTime} = ets_cache_options:life_time(Name),
	    [{max_size, MaxSize},
	     {cache_missed, CacheMissed},
	     {life_time, LifeTime}
	     |lists:filtermap(
		fun({memory, V}) ->
			{true, {memory, V * erlang:system_info(wordsize)}};
		   ({owner, _}) -> true;
		   ({name, _}) -> true;
		   ({size, _}) -> true;
		   (_) -> false
		end, ETSInfo)]
    end.

-spec info(atom(), atom()) -> any().
info(Name, Option) ->
    Options = info(Name),
    case lists:keyfind(Option, 1, Options) of
	{_, Value} -> Value;
	_ -> erlang:error(badarg)
    end.

load_nif() ->
    Path = p1_nif_utils:get_so_path(?MODULE, [?MODULE], atom_to_list(?MODULE)),
    MaxTables = get_max_tables(),
    case erlang:load_nif(Path, MaxTables) of
	ok -> ok;
	{error, {Reason, Text}} ->
	    error_logger:error_msg("Failed to load NIF ~s: ~s (~p)",
				   [Path, Text, Reason]),
	    erlang:nif_error(Reason)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Name, Opts]) ->
    process_flag(trap_exit, true),
    ets:new(Name, [named_table, public, {read_concurrency, true}]),
    Counter = new_counter(),
    TRef = start_clean_timer(),
    State = #state{tab = Name, clean_timer = TRef},
    {ok, do_setopts(State, [{counter, Counter}|Opts])}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({setopts, Opts}, State) ->
    {noreply, do_setopts(State, Opts)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({insert, Op, {Key, _, CurrTime} = New}, #state{tab = Tab} = State) ->
    case ets:lookup(Tab, Key) of
	[Old] ->
	    case delete_if_expired(Tab, Old, CurrTime) of
		true ->
		    do_insert(Op, Tab, New);
		false ->
		    ok
	    end;
	[] ->
	    do_insert(Op, Tab, New)
    end,
    {noreply, State};
handle_info({delete, Key}, #state{tab = Tab} = State) ->
    ets_delete(Tab, Key),
    {noreply, State};
handle_info(clear, #state{tab = Tab} = State) ->
    ets_delete_all_objects(Tab),
    {noreply, State};
handle_info(clean, #state{clean_timer = TRef} = State) ->
    clean_expired(State),
    NewTRef = start_clean_timer(TRef),
    {noreply, State#state{clean_timer = NewTRef}};
handle_info(table_overflow, #state{tab = Tab, max_size = MaxSize} = State) ->
    case ets:info(Tab, size) of
	N when N >= MaxSize ->
	    error_logger:warning_msg(
	      "Shrinking cache '~s' (current size = ~B, max size = ~B, "
	      "life time = ~p, memory = ~B bytes); you should increase maximum "
	      "cache size if this message repeats too often",
	      [Tab, N, MaxSize, State#state.life_time,
	       ets:info(Tab, memory) * erlang:system_info(wordsize)]),
	    ets_delete_all_objects(Tab);
	_ ->
	    ok
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{tab = Tab}) ->
    delete_counter(Tab).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec check_size(atom(), pos_integer() | infinity) -> ok.
check_size(_Tab, infinity) ->
    ok;
check_size(Tab, MaxSize) ->
    try ets:info(Tab, size) of
	N when N >= MaxSize ->
	    send({Tab, node()}, table_overflow);
	_ ->
	    ok
    catch _:badarg ->
	    ok
    end.

-spec insert(new | replace, atom(), any(), any(), [node()]) -> boolean().
insert(Op, Name, Key, Val, Nodes) ->
    CurrTime = current_time(),
    Obj = {Key, {ok, Val}, CurrTime},
    lists:foldl(
      fun(Node, Result) when Node /= node() ->
	      send({Name, Node}, {insert, Op, Obj}),
	      Result;
	 (_, _) ->
	      try ets:lookup(Name, Key) of
		  [Old] ->
		      case delete_if_expired(Name, Old, CurrTime) of
			  true -> do_insert(Op, Name, Obj);
			  false -> false
		      end;
		  [] -> do_insert(Op, Name, Obj)
	      catch _:badarg ->
		      false
	      end
      end, false, Nodes).

do_insert(Op, Name, Obj) ->
    {ok, CacheMissed} = ets_cache_options:cache_missed(Name),
    {ok, MaxSize} = ets_cache_options:max_size(Name),
    do_insert(Op, Name, Obj, CacheMissed, MaxSize).

do_insert(_Op, _Name, {_Key, error, _Time}, _CacheMissed = false, _MaxSize) ->
    false;
do_insert(Op, Name, Obj, _CacheMissed, MaxSize) ->
    check_size(Name, MaxSize),
    try
	case Op of
	    new -> ets:insert_new(Name, Obj);
	    replace -> ets:insert(Name, Obj)
	end
    catch _:badarg ->
	    false
    end.

-spec do_filter(atom(), filter_fun(), pos_integer() | infinity,
		integer(), non_neg_integer(), any()) -> non_neg_integer().
do_filter(_Name, _FilterFun, _LifeTime, _CurrTime, Num, '$end_of_table') ->
    Num;
do_filter(Name, FilterFun, LifeTime, CurrTime, Num, Key) ->
    NewNum = try ets:lookup(Name, Key) of
		 [{Key, Val, Time}] ->
		     if ?IS_EXPIRED(Time, LifeTime, CurrTime) ->
			     ets_delete_object(Name, {Key, Val, Time}),
			     Num;
			true ->
			     case FilterFun(Key, Val) of
				 true ->
				     Num;
				 false ->
				     ets_delete_object(Name, {Key, Val, Time}),
				     Num + 1
			     end
		     end;
		 _ ->
		     Num
	     catch _:badarg ->
		     Num
	     end,
    do_filter(Name, FilterFun, LifeTime, CurrTime, NewNum, ets_next(Name, Key)).

-spec do_fold(atom(), fold_fun(), pos_integer() | infinity,
	      integer(), any(), any()) -> any().
do_fold(_Name, _FoldFun, _LifeTime, _CurrTime, Acc, '$end_of_table') ->
    Acc;
do_fold(Name, FoldFun, LifeTime, CurrTime, Acc, Key) ->
    NewAcc = try ets:lookup(Name, Key) of
		 [{Key, Val, Time}] ->
		     if ?IS_EXPIRED(Time, LifeTime, CurrTime) ->
			     ets_delete_object(Name, {Key, Val, Time}),
			     Acc;
			true ->
			     FoldFun(Key, Val, Acc)
		     end;
		 _ ->
		     Acc
	     catch _:badarg ->
		     Acc
	     end,
    do_fold(Name, FoldFun, LifeTime, CurrTime, NewAcc, ets_next(Name, Key)).

-spec check_opts([option()]) -> [option()].
check_opts(Opts) ->
    lists:map(
      fun({Name, Val} = Opt) ->
	      case {Name, Val} of
		  {max_size, Val} when is_integer(Val), Val>0 -> Opt;
		  {max_size, infinity} -> Opt;
		  {life_time, Val} when is_integer(Val), Val>0 -> Opt;
		  {life_time, infinity} -> Opt;
		  {cache_missed, Bool} when is_boolean(Bool) -> Opt;
		  _ -> erlang:error({bad_option, Opt})
	      end
      end, Opts).

-spec do_setopts(state(), [{counter, counter()} | option()]) -> state().
do_setopts(State, Opts) ->
    MaxSize = proplists:get_value(max_size, Opts, State#state.max_size),
    LifeTime = proplists:get_value(life_time, Opts, State#state.life_time),
    CacheMissed = proplists:get_value(cache_missed, Opts, State#state.cache_missed),
    Counter = proplists:get_value(counter, Opts, State#state.counter),
    NewState = State#state{max_size = MaxSize,
			   counter = Counter,
			   life_time = LifeTime,
			   cache_missed = CacheMissed},
    if State == NewState ->
	    ok;
       true ->
	    Tab = State#state.tab,
	    p1_options:insert(ets_cache_options, max_size, Tab, MaxSize),
	    p1_options:insert(ets_cache_options, life_time, Tab, LifeTime),
	    p1_options:insert(ets_cache_options, cache_missed, Tab, CacheMissed),
	    p1_options:insert(ets_cache_options, counter, Tab, Counter),
	    p1_options:compile(ets_cache_options)
    end,
    NewState.

-spec current_time() -> integer().
current_time() ->
    p1_time_compat:monotonic_time(milli_seconds).

-spec start_clean_timer() -> reference().
start_clean_timer() ->
    start_clean_timer(make_ref()).

-spec start_clean_timer(reference()) -> reference().
start_clean_timer(TRef) ->
    case erlang:cancel_timer(TRef) of
	false ->
	    receive {timeout, TRef, _} -> ok
	    after 0 -> ok end;
	_ -> ok
    end,
    Timeout = timer:minutes(1) + p1_rand:uniform(timer:minutes(4)),
    erlang:send_after(Timeout, self(), clean).

-spec clean_expired(state()) -> non_neg_integer().
clean_expired(#state{tab = Tab, life_time = LifeTime}) ->
    CurrTime = current_time(),
    ets_select_delete(
      Tab, ets:fun2ms(
	     fun({_, _, Time}) ->
		     ?IS_EXPIRED(Time, LifeTime, CurrTime)
	     end)).

delete_if_expired(Name, Obj) ->
    case ets_cache_options:life_time(Name) of
	{ok, infinity} ->
	    false;
	{ok, LifeTime} ->
	    delete_if_expired(Name, Obj, current_time(), LifeTime)
    end.

delete_if_expired(Name, Obj, CurrTime) ->
    {ok, LifeTime} = ets_cache_options:life_time(Name),
    delete_if_expired(Name, Obj, CurrTime, LifeTime).

delete_if_expired(Name, {_, _, Time} = Obj, CurrTime, LifeTime) ->
    if ?IS_EXPIRED(Time, LifeTime, CurrTime) ->
	    ets_delete_object(Name, Obj);
       true ->
	    false
    end.

send(Dst, Msg) ->
    erlang:send(Dst, Msg, [noconnect, nosuspend]).

-spec ets_delete(atom(), any()) -> boolean().
ets_delete(Tab, Key) ->
    try
	incr_counter(Tab),
	ets:delete(Tab, Key)
    catch _:badarg ->
	    false;
	  _:{badmatch, undefined} ->
	    false
    end.

-spec ets_delete_object(atom(), any()) -> boolean().
ets_delete_object(Tab, Obj) ->
    try
	incr_counter(Tab),
	ets:delete_object(Tab, Obj)
    catch _:badarg ->
	    false;
	  _:{badmatch, undefined} ->
	    false
    end.

-spec ets_delete_all_objects(atom()) -> boolean().
ets_delete_all_objects(Tab) ->
    try
	incr_counter(Tab),
	ets:delete_all_objects(Tab)
    catch _:badarg ->
	    false;
	  _:{badmatch, undefined} ->
	    false
    end.

-spec ets_select_delete(atom(), fun()) -> non_neg_integer().
ets_select_delete(Tab, Fun) ->
    try
	incr_counter(Tab),
	ets:select_delete(Tab, Fun)
    catch _:badarg ->
	    0
    end.

-spec ets_first(atom()) -> any().
ets_first(Tab) ->
    try ets:first(Tab)
    catch _:badarg -> '$end_of_table'
    end.

-spec ets_next(atom(), any()) -> any().
ets_next(Tab, Key) ->
    try ets:next(Tab, Key)
    catch _:badarg -> '$end_of_table'
    end.

-spec ets_safe_fixtable(atom(), boolean()) -> boolean().
ets_safe_fixtable(Tab, Fix) ->
    try ets:safe_fixtable(Tab, Fix)
    catch _:badarg -> false
    end.

get_max_tables() ->
    case application:get_env(cache_tab, max_cache_tables) of
	undefined -> ?DEFAULT_MAX_CACHE_TABLES;
	{ok, I} when is_integer(I), I > 0 -> I;
	{ok, Val} ->
	    error_logger:error_msg(
	      "Incorrect value of 'max_cache_tables' "
	      "application parameter: ~p", [Val]),
	    ?DEFAULT_MAX_CACHE_TABLES
    end.

%%%===================================================================
%%% Counters
%%%===================================================================
-spec new_counter() -> counter().
new_counter() ->
    case new_counter_nif() of
	{ok, Counter} ->
	    Counter;
	{error, {system_limit, MaxTables}} ->
	    error_logger:error_msg(
	      "Maximum number (~B) of ETS cache tables has reached",
	      [MaxTables]),
	    erlang:error(system_limit)
    end.

-spec get_counter(atom()) -> non_neg_integer().
get_counter(Name) ->
    {ok, Counter} = ets_cache_options:counter(Name),
    get_counter_nif(Counter).

-spec incr_counter(atom()) -> non_neg_integer().
incr_counter(Name) ->
    {ok, Counter} = ets_cache_options:counter(Name),
    incr_counter_nif(Counter).

-spec delete_counter(counter()) -> ok.
delete_counter(Name) ->
    {ok, Counter} = ets_cache_options:counter(Name),
    delete_counter_nif(Counter).

-spec new_counter_nif() -> {ok, counter()} | {error, {system_limit, pos_integer()}}.
new_counter_nif() ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec get_counter_nif(counter()) -> non_neg_integer().
get_counter_nif(_) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec incr_counter_nif(counter()) -> non_neg_integer().
incr_counter_nif(_) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

-spec delete_counter_nif(counter()) -> ok.
delete_counter_nif(_) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).
