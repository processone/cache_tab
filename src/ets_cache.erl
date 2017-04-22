%%%-------------------------------------------------------------------
%%% @author Evgeny Khramtsov <ekhramtsov@process-one.net>
%%% Created : 12 Apr 2017 by Evgeny Khramtsov <ekhramtsov@process-one.net>
%%%
%%%
%%% ejabberd, Copyright (C) 2002-2017   ProcessOne
%%%
%%% This program is free software; you can redistribute it and/or
%%% modify it under the terms of the GNU General Public License as
%%% published by the Free Software Foundation; either version 2 of the
%%% License, or (at your option) any later version.
%%%
%%% This program is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%%% General Public License for more details.
%%%
%%% You should have received a copy of the GNU General Public License along
%%% with this program; if not, write to the Free Software Foundation, Inc.,
%%% 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
%%%
%%%-------------------------------------------------------------------
-module(ets_cache).
-behaviour(gen_server).

%% API
-export([new/1, new/2, start_link/2, delete/1, delete/2, delete/3,
	 lookup/2, lookup/3, insert/3, insert/4, insert_new/3, insert_new/4,
	 setopts/2, clear/1, clear/2, filter/2, all/0, info/1, info/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include_lib("stdlib/include/ms_transform.hrl").

-record(state, {tab :: atom(),
		cache_missed :: boolean(),
		life_time :: milli_seconds() | infinity,
		max_size :: pos_integer() | infinity}).

-define(CALL_TIMEOUT, timer:minutes(1)).

-type milli_seconds() :: pos_integer().
-type option() :: {max_size, pos_integer() | infinity} |
		  {life_time, milli_seconds() | infinity} |
		  {cache_missed, boolean()}.
-type read_fun() :: fun(() -> {ok, any()} | error).
-type filter_fun() :: fun((any(), {ok, any()} | error) -> boolean()).
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
    Spec = {Name, {?MODULE, start_link, [Name, Opts1]},
	    transient, 5000, worker, [?MODULE]},
    case supervisor:start_child(cache_tab_sup, Spec) of
	{ok, _Pid} -> ok;
	{error, {already_started, _}} -> setopts(Name, Opts1);
	{error, Err} -> erlang:error(Err)
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
    case whereis(Name) of
	undefined -> ok;
	_ ->
	    lists:foreach(
	      fun(Node) when Node /= node() ->
		      send({Name, Node}, {delete, Key});
		 (_MyNode) ->
		      ets:delete(Name, Key)
	      end, Nodes)
    end.

-spec insert(atom(), any(), any()) -> ok.
insert(Name, Key, Val) ->
    insert(Name, Key, Val, [node()]).

insert(Name, Key, Val, Nodes) ->
    case whereis(Name) of
	undefined -> ok;
	_ ->
	    lists:foreach(
	      fun(Node) when Node /= node() ->
		      send({Name, Node}, {insert, Key, {ok, Val}});
		 (_MyNode) ->
		      gen_server:call(Name, {insert, Key, {ok, Val}},
				      ?CALL_TIMEOUT)
	      end, Nodes)
    end.

-spec insert_new(atom(), any(), any()) -> boolean().
insert_new(Name, Key, Val) ->
    insert_new(Name, Key, Val, [node()]).

insert_new(Name, Key, Val, Nodes) ->
    case whereis(Name) of
	undefined -> false;
	_ ->
	    lists:foldl(
	      fun(Node, Result) when Node /= node() ->
		      send({Name, Node}, {insert_new, Key, {ok, Val}}),
		      Result;
		 (_MyNode, _) ->
		      gen_server:call(Name, {insert_new, Key, {ok, Val}},
				      ?CALL_TIMEOUT)
	      end, false, Nodes)
    end.

-spec lookup(atom(), any()) -> {ok, any()} | error.
lookup(Name, Key) ->
    lookup(Name, Key, undefined).

-spec lookup(atom(), any(), read_fun() | undefined) -> {ok, any()} | error.
lookup(Name, Key, ReadFun) ->
    try ets:lookup(Name, Key) of
	[{_, Val, ExpireTime}] ->
	    if is_integer(ExpireTime) ->
		    case ExpireTime > current_time() of
			true -> Val;
			false ->
			    ets:delete_object(Name, {Key, Val, ExpireTime}),
			    do_lookup(Name, Key, ReadFun)
		    end;
	       true ->
		    Val
	    end;
	[] ->
	    do_lookup(Name, Key, ReadFun);
	_ when ReadFun /= undefined ->
	    ReadFun();
	_ ->
	    error
    catch _:badarg when ReadFun /= undefined ->
	    ReadFun();
	  _:badarg ->
	    error
    end.

-spec clear(atom()) -> ok.
clear(Name) ->
    clear(Name, [node()]).

-spec clear(atom(), [atom()]) -> ok.
clear(Name, Nodes) ->
    case whereis(Name) of
	undefined -> ok;
	_ ->
	    lists:foreach(
	      fun(Node) when Node /= node() ->
		      send({Name, Node}, clear);
		 (_MyNode) ->
		      ets:delete_all_objects(Name)
	      end, Nodes)
    end.

-spec filter(atom(), filter_fun()) -> ok.
filter(Name, FilterFun) ->
    case whereis(Name) of
	undefined -> ok;
	_ ->
	    ets:safe_fixtable(Name, true),
	    do_filter(Name, FilterFun, current_time(), ets:first(Name)),
	    ets:safe_fixtable(Name, false),
	    ok
    end.

-spec all() -> [atom()].
all() ->
    [Child || {Child, _, _, [?MODULE]} <- supervisor:which_children(cache_tab_sup)].

-spec info(atom()) -> [proplists:property()].
info(Name) ->
    ETSInfo = ets:info(Name),
    State = sys:get_state(Name),
    [{max_size, State#state.max_size},
     {cache_missed, State#state.cache_missed},
     {life_time, State#state.life_time}
     |lists:filter(
	fun({read_concurrency, _}) -> true;
	   ({write_concurrency, _}) -> true;
	   ({memory, _}) -> true;
	   ({owner, _}) -> true;
	   ({name, _}) -> true;
	   ({size, _}) -> true;
	   (_) -> false
	end, ETSInfo)].

-spec info(atom(), atom()) -> any().
info(Name, Option) ->
    Options = info(Name),
    case lists:keyfind(Option, 1, Options) of
	{_, Value} -> Value;
	_ -> erlang:error(badarg)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Name, Opts]) ->
    ets:new(Name, [named_table, public, {read_concurrency, true}]),
    MaxSize = proplists:get_value(max_size, Opts, infinity),
    CacheMissed = proplists:get_value(cache_missed, Opts, true),
    LifeTime = proplists:get_value(life_time, Opts, infinity),
    start_clean_timer(),
    {ok, #state{tab = Name,
		max_size = MaxSize,
		life_time = LifeTime,
		cache_missed = CacheMissed}}.

handle_call({insert, Key, Val}, _From, State) ->
    do_insert(State, Key, Val),
    {reply, ok, State};
handle_call({insert_new, Key, Val}, _From, State) ->
    {reply, do_insert_new(State, Key, Val), State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({setopts, Opts}, State) ->
    MaxSize = proplists:get_value(max_size, Opts, State#state.max_size),
    CacheMissed = proplists:get_value(cache_missed, Opts, State#state.cache_missed),
    LifeTime = proplists:get_value(life_time, Opts, State#state.life_time),
    {noreply, State#state{max_size = MaxSize,
			  life_time = LifeTime,
			  cache_missed = CacheMissed}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({insert, Ref, Key, error}, #state{cache_missed = false} = State) ->
    ets:delete_object(State#state.tab, {Key, Ref}),
    {noreply, State};
handle_info({insert, Ref, Key, Val}, #state{tab = Tab} = State) ->
    case ets:lookup(Tab, Key) of
	[{_, Ref}] ->
	    do_insert(State, Key, Val);
	_ ->
	    ok
    end,
    {noreply, State};
handle_info({insert, Key, Val}, State) ->
    do_insert(State, Key, Val),
    {noreply, State};
handle_info({insert_new, Key, Val}, State) ->
    do_insert_new(State, Key, Val),
    {noreply, State};
handle_info({delete, Key}, #state{tab = Tab} = State) ->
    ets:delete(Tab, Key),
    {noreply, State};
handle_info(clear, #state{tab = Tab} = State) ->
    ets:delete_all_objects(Tab),
    {noreply, State};
handle_info({timeout, _TRef, clean}, State) ->
    clean_expired(State),
    start_clean_timer(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec check_size(atom(), non_neg_integer() | infinity) -> true.
check_size(Tab, MaxSize) when is_integer(MaxSize) ->
    case ets:info(Tab, size) of
	N when N >= MaxSize ->
	    error_logger:warning_msg("shrinking ~s table", [Tab]),
	    ets:delete_all_objects(Tab);
	_ ->
	    true
    end;
check_size(_, _) ->
    true.

-spec do_filter(atom(), filter_fun(), integer(), any()) -> ok.
do_filter(_Name, _FilterFun, _CurrTime, '$end_of_table') ->
    ok;
do_filter(Name, FilterFun, CurrTime, Key) ->
    case ets:lookup(Name, Key) of
	[{Key, Val, ExpireTime}] when ExpireTime > CurrTime ->
	    case FilterFun(Key, Val) of
		true -> ok;
		false -> ets:delete_object(Name, {Key, Val, ExpireTime})
	    end;
	_ ->
	    ok
    end,
    do_filter(Name, FilterFun, CurrTime, ets:next(Name, Key)).

-spec do_lookup(atom(), any(), read_fun() | undefined) -> {ok, any()} | error.
do_lookup(_Name, _Key, undefined) ->
    error;
do_lookup(Name, Key, ReadFun) ->
    Ref = make_ref(),
    case ets:insert_new(Name, {Key, Ref}) of
	true ->
	    try
		Val = ReadFun(),
		case Val of
		    {ok, _} ->
			send(Name, {insert, Ref, Key, Val});
		    error ->
			send(Name, {insert, Ref, Key, Val});
		    _Other ->
			ets:delete_object(Name, {Key, Ref})
		end,
		Val
	    catch E:R ->
		    catch ets:delete_object(Name, {Key, Ref}),
		    erlang:raise(E, R, erlang:get_stacktrace())
	    end;
	false ->
	    ReadFun()
    end.

-spec do_insert(state(), any(), {ok, any()} | error) -> ok.
do_insert(State, Key, Val) ->
    check_size(State#state.tab, State#state.max_size),
    LifeTime = State#state.life_time,
    ExpireTime = if is_integer(LifeTime) ->
			 current_time() + LifeTime;
		    true ->
			 LifeTime
		 end,
    ets:insert(State#state.tab, {Key, Val, ExpireTime}),
    ok.

-spec do_insert_new(state(), any(), {ok, any()} | error) -> boolean().
do_insert_new(State, Key, Val) ->
    Tab = State#state.tab,
    LifeTime = State#state.life_time,
    check_size(Tab, State#state.max_size),
    if is_integer(LifeTime) ->
	    CurrTime = current_time(),
	    case ets:lookup(State#state.tab, Key) of
		[{Key, Val, ExpireTime}] when ExpireTime > CurrTime ->
		    ets:delete_object(Tab, {Key, Val, ExpireTime}),
		    ets:insert_new(Tab, {Key, Val, LifeTime + CurrTime});
		[] ->
		    ets:insert_new(Tab, {Key, Val, LifeTime + CurrTime});
		_ ->
		    false
	    end;
       true ->
	    ets:insert_new(Tab, {Key, Val, LifeTime})
    end.

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

-spec current_time() -> integer().
current_time() ->
    p1_time_compat:monotonic_time(milli_seconds).

-spec start_clean_timer() -> reference().
start_clean_timer() ->
    Timeout = crypto:rand_uniform(timer:minutes(1), timer:minutes(5)),
    erlang:start_timer(Timeout, self(), clean).

-spec clean_expired(state()) -> non_neg_integer().
clean_expired(#state{life_time = infinity}) ->
    0;
clean_expired(#state{tab = Tab}) ->
    CurrTime = current_time(),
    ets:select_delete(
      Tab, ets:fun2ms(
	     fun({_, _, ExpireTime}) ->
		     ExpireTime =< CurrTime
	     end)).

send(Dst, Msg) ->
    erlang:send(Dst, Msg, [noconnect, nosuspend]).
