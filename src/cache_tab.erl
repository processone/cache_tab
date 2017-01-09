%%%-------------------------------------------------------------------
%%% File    : cache_tab.erl
%%% Author  : Evgeniy Khramtsov <ekhramtsov@process-one.net>
%%% Description : Caching key-value table
%%%
%%% Created : 29 Aug 2010 by Evgeniy Khramtsov <ekhramtsov@process-one.net>
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
-module(cache_tab).

-define(GEN_SERVER, gen_server).

-behaviour(?GEN_SERVER).

%% API
-export([start_link/4, new/2, delete/1, delete/3, lookup/3,
	 insert/4, info/2, tab2list/1, setopts/2,
	 dirty_lookup/3,
         dirty_insert/4, dirty_insert/3, dirty_dist_insert/4,
         dirty_delete/3, dirty_delete/2, dirty_dist_delete/3,
	 all/0, clean/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, {tab = treap:empty(),
		name,
		size = 0,
		owner,
		max_size,
		life_time,
		warn,
		hits = 0,
		miss = 0,
		procs_num,
		cache_missed,
		lru,
		shrink_size}).

-define(PROCNAME, ?MODULE).
-define(CALL_TIMEOUT, 60000).

%% Defaults
-define(MAX_SIZE, 1000).
-define(WARN, true).
-define(CACHE_MISSED, true).
-define(LRU, true).
-define(LIFETIME, 600). %% 10 minutes

%%====================================================================
%% API
%%====================================================================
start_link(Proc, Tab, Opts, Owner) ->
    ?GEN_SERVER:start_link(
      {local, Proc}, ?MODULE, [Tab, Opts, get_proc_num(), Owner], []).

new(Tab, Opts) ->
    Res = lists:flatmap(
	    fun(Proc) ->
		    Spec = {{Tab, Proc},
			    {?MODULE, start_link,
			     [Proc, Tab, Opts, self()]},
			    permanent,
			    brutal_kill,
			    worker,
			    [?MODULE]},
		    case supervisor:start_child(cache_tab_sup, Spec) of
			{ok, _Pid} ->
			    [ok];
			R ->
			    [R]
		    end
	    end, get_all_procs(Tab)),
    case lists:filter(fun(ok) -> false; (_) -> true end, Res) of
	[] ->
	    ok;
	Err ->
	    {error, Err}
    end.

clean(Tab) ->
    lists:foreach(
      fun(Proc) ->
	      ?GEN_SERVER:call(Proc, clean, ?CALL_TIMEOUT)
      end, get_all_procs(Tab)).

delete(Tab) ->
    lists:foreach(
      fun(Proc) ->
	      supervisor:terminate_child(cache_tab_sup, {Tab, Proc}),
	      supervisor:delete_child(cache_tab_sup, {Tab, Proc})
      end, get_all_procs(Tab)).

delete(Tab, Key, F) ->
    ?GEN_SERVER:call(
       get_proc_by_hash(Tab, Key), {delete, Key, F}, ?CALL_TIMEOUT).

dirty_delete(Tab, Key, F) ->
    F(),
    ?GEN_SERVER:call(
       get_proc_by_hash(Tab, Key), {cache_delete, Key}, ?CALL_TIMEOUT).

dirty_delete(Tab, Key) ->
    ?GEN_SERVER:call(
       get_proc_by_hash(Tab, Key), {cache_delete, Key}, ?CALL_TIMEOUT).

dirty_dist_delete(Tab, Key, F) ->
    F(),
    rpc:eval_everywhere(
      cache_tab_app:get_nodes() -- [node()],
      ?MODULE, dirty_delete, [Tab, Key]),
    dirty_delete(Tab, Key).

lookup(Tab, Key, F) ->
    ?GEN_SERVER:call(
       get_proc_by_hash(Tab, Key), {lookup, Key, F}, ?CALL_TIMEOUT).

dirty_lookup(Tab, Key, F) ->
    Proc = get_proc_by_hash(Tab, Key),
    case ?GEN_SERVER:call(Proc, {cache_lookup, Key}, ?CALL_TIMEOUT) of
	{ok, '$cached_mismatch'} ->
	    error;
	{ok, Val} ->
	    {ok, Val};
	_ ->
	    {Result, NewVal} = case F() of
				   {ok, Val} ->
				       {{ok, Val}, Val};
				   _ ->
				       {error, '$cached_mismatch'}
			       end,
	    ?GEN_SERVER:call(
	       Proc, {cache_insert, Key, NewVal}, ?CALL_TIMEOUT),
	    Result
    end.

insert(Tab, Key, Val, F) ->
    ?GEN_SERVER:call(
       get_proc_by_hash(Tab, Key), {insert, Key, Val, F}, ?CALL_TIMEOUT).

dirty_insert(Tab, Key, Val, F) ->
    F(),
    ?GEN_SERVER:call(
       get_proc_by_hash(Tab, Key), {cache_insert, Key, Val}, ?CALL_TIMEOUT).

dirty_insert(Tab, Key, Val) ->
    ?GEN_SERVER:call(
       get_proc_by_hash(Tab, Key), {cache_insert, Key, Val}, ?CALL_TIMEOUT).

dirty_dist_insert(Tab, Key, Val, F) ->
    F(),
    rpc:eval_everywhere(
      cache_tab_app:get_nodes() -- [node()],
      ?MODULE, dirty_insert, [Tab, Key, Val]),
    dirty_insert(Tab, Key, Val).

info(Tab, Info) ->
    case lists:map(
	   fun(Proc) ->
		   ?GEN_SERVER:call(Proc, {info, Info}, ?CALL_TIMEOUT)
	   end, get_all_procs(Tab)) of
	Res when Info == size ->
	    {ok, lists:sum(Res)};
	Res when Info == all ->
	    {ok, Res};
	Res when Info == ratio ->
	    {H, M} = lists:foldl(
		       fun({Hits, Miss}, {HitsAcc, MissAcc}) ->
			       {HitsAcc + Hits, MissAcc + Miss}
		       end, {0, 0}, Res),
	    {ok, [{hits, H}, {miss, M}]};
	_ ->
	    {error, badarg}
    end.

setopts(Tab, Opts) ->
    lists:foreach(
      fun(Proc) ->
	      ?GEN_SERVER:call(Proc, {setopts, Opts}, ?CALL_TIMEOUT)
      end, get_all_procs(Tab)).

tab2list(Tab) ->
    lists:flatmap(
      fun(Proc) ->
	      ?GEN_SERVER:call(Proc, tab2list, ?CALL_TIMEOUT)
      end, get_all_procs(Tab)).

all() ->
    lists:usort(
      [Tab || {{Tab, _}, _, _, _} <- supervisor:which_children(cache_tab_sup)]).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Tab, Opts, N, Pid]) ->
    State = #state{procs_num = N,
		   owner = Pid,
		   name = Tab},
    {ok, do_setopts(State, Opts)}.

handle_call({lookup, Key, F}, _From, #state{tab = T} = State) ->
    CleanPrio = clean_priority(State#state.life_time),
    case treap:lookup(Key, T) of
	{ok, Prio, Val} when (State#state.lru == true) or (Prio =< CleanPrio) ->
	    Hits = State#state.hits,
	    NewState = treap_update(Key, Val, State#state{hits = Hits + 1}),
	    case Val of
		'$cached_mismatch' ->
		    {reply, error, NewState};
		_ ->
		    {reply, {ok, Val}, NewState}
	    end;
	_ ->
	    case catch F() of
		{ok, Val} ->
		    Miss = State#state.miss,
		    NewState = treap_insert(Key, Val, State),
		    {reply, {ok, Val}, NewState#state{miss = Miss + 1}};
		{'EXIT', Reason} ->
		    print_error(lookup, [Key], Reason, State),
		    {reply, error, State};
		_ ->
		    Miss = State#state.miss,
		    NewState = State#state{miss = Miss + 1},
		    if State#state.cache_missed ->
			    {reply, error,
			     treap_insert(Key, '$cached_mismatch', NewState)};
		       true ->
			    {reply, error, NewState}
		    end
	    end
    end;
handle_call({cache_lookup, Key}, _From, #state{tab = T} = State) ->
    CleanPrio = clean_priority(State#state.life_time),
    case treap:lookup(Key, T) of
	{ok, Prio, Val} when (State#state.lru == true) or (Prio =< CleanPrio) ->
	    Hits = State#state.hits,
	    NewState = treap_update(Key, Val, State#state{hits = Hits + 1}),
	    {reply, {ok, Val}, NewState};
	_ ->
	    Miss = State#state.miss,
	    NewState = State#state{miss = Miss + 1},
	    {reply, error, NewState}
    end;
handle_call({insert, Key, Val, F}, _From, #state{tab = T} = State) ->
    case treap:lookup(Key, T) of
	{ok, _Prio, Val} ->
	    {reply, ok, treap_update(Key, Val, State)};
	_ ->
	    case catch F() of
		{'EXIT', Reason} ->
		    print_error(insert, [Key, Val], Reason, State),
		    {reply, ok, State};
		_ ->
		    {reply, ok, treap_insert(Key, Val, State)}
	    end
    end;
handle_call({cache_insert, _, '$cached_mismatch'}, _From,
	    #state{cache_missed = false} = State) ->
    {reply, ok, State};
handle_call({cache_insert, Key, Val}, _From, State) ->
    {reply, ok, treap_insert(Key, Val, State)};
handle_call({delete, Key, F}, _From, State) ->
    NewState = treap_delete(Key, State),
    case catch F() of
	{'EXIT', Reason} ->
	    print_error(delete, [Key], Reason, State);
	_ ->
	    ok
    end,
    {reply, ok, NewState};
handle_call({cache_delete, Key}, _From, State) ->
    NewState = treap_delete(Key, State),
    {reply, ok, NewState};
handle_call({info, Info}, _From, State) ->
    Res = case Info of
	      size ->
		  State#state.size;
	      ratio ->
		  {State#state.hits, State#state.miss};
	      all ->
		  [{max_size, State#state.max_size},
		   {life_time, State#state.life_time},
		   {shrink_size, State#state.shrink_size},
		   {size, State#state.size},
		   {owner, State#state.owner},
		   {hits, State#state.hits},
		   {miss, State#state.miss},
		   {cache_missed, State#state.cache_missed},
		   {lru, State#state.lru},
		   {warn, State#state.warn}];
	      _ ->
		  badarg
	  end,
    {reply, Res, State};
handle_call(tab2list, _From, #state{tab = T} = State) ->
    Res = treap:fold(
	    fun({Key, _, Val}, Acc) ->
		    [{Key, Val}|Acc]
	    end, [], T),
    {reply, Res, State};
handle_call({setopts, Opts}, _From, State) ->
    {reply, ok, do_setopts(State, Opts)};
handle_call(clean, _From, State) ->
    {reply, ok, State#state{tab = treap:empty(), size = 0}};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
do_setopts(#state{procs_num = N} = State, Opts) ->
    MaxSize = case {proplists:get_value(max_size, Opts),
		    State#state.max_size} of
		  {MS, _} when is_integer(MS), MS > 0 ->
		      round(MS/N);
		  {unlimited, _} ->
		      unlimited;
		  {_, undefined} ->
		      round(?MAX_SIZE/N);
		  {_, MS} ->
		      MS
	      end,
    LifeTime = case {proplists:get_value(life_time, Opts),
		     State#state.life_time} of
		   {LT, _} when is_integer(LT), LT > 0 ->
		       LT*1000*1000;
		   {unlimited, _} ->
		       unlimited;
		   {_, undefined} ->
		       ?LIFETIME*1000*1000;
		   {_, LT} ->
		       LT
	       end,
    ShrinkSize = case {proplists:get_value(shrink_size, Opts),
		       State#state.shrink_size} of
		     {SS, _} when is_integer(SS), SS > 0 ->
			 round(SS/N);
		     _ when is_integer(MaxSize) ->
			 round(MaxSize/2);
		     _ ->
			 unlimited
		 end,
    Warn = case {proplists:get_value(warn, Opts),
		 State#state.warn} of
	       {true, _} ->
		   true;
	       {false, _} ->
		   false;
	       {_, undefined} ->
		   ?WARN;
	       {_, W} ->
		   W
	   end,
    CacheMissed = case proplists:get_value(
			 cache_missed, Opts, State#state.cache_missed) of
		      false ->
			  false;
		      true ->
			  true;
		      _ ->
			  ?CACHE_MISSED
		  end,
    LRU = case proplists:get_value(
		 lru, Opts, State#state.lru) of
	      false ->
		  false;
	      true ->
		  true;
	      _ ->
		  ?LRU
	  end,
    State#state{max_size = MaxSize,
		warn = Warn,
		life_time = LifeTime,
		cache_missed = CacheMissed,
		lru = LRU,
		shrink_size = ShrinkSize}.

get_proc_num() ->
    case catch erlang:system_info(logical_processors) of
        Num when is_integer(Num) ->
            Num;
        _ ->
            1
    end.

get_proc_by_hash(Tab, Term) ->
    N = erlang:phash2(Term, get_proc_num()) + 1,
    get_proc(Tab, N).

get_proc(Tab, N) ->
    list_to_atom(atom_to_list(?PROCNAME) ++ "_" ++
		 atom_to_list(Tab) ++ "_" ++ integer_to_list(N)).

get_all_procs(Tab) ->
    [get_proc(Tab, N) || N <- lists:seq(1, get_proc_num())].

now_priority() ->
    -p1_time_compat:system_time(micro_seconds).

clean_priority(LifeTime) ->
    if is_integer(LifeTime) ->
	    now_priority() + LifeTime;
       true ->
	    unlimited
    end.

treap_update(Key, Val, #state{tab = T, lru = LRU} = State) ->
    if LRU ->
	    Priority = now_priority(),
	    NewT = treap:insert(Key, Priority, Val, T),
	    State#state{tab = NewT};
       true ->
	    State
    end.

treap_insert(Key, Val, State) ->
    State1 = clean_treap(State),
    #state{size = Size} = State2 = shrink_treap(State1),
    T = State2#state.tab,
    case treap:lookup(Key, T) of
	{ok, _, Val} ->
	    treap_update(Key, Val, State2);
	{ok, _, _} ->
	    NewT = treap:insert(Key, now_priority(), Val, T),
	    State2#state{tab = NewT};
	_ ->
	    NewT = treap:insert(Key, now_priority(), Val, T),
	    State2#state{tab = NewT, size = Size+1}
    end.

treap_delete(Key, #state{tab = T, size = Size} = State) ->
    case treap:lookup(Key, T) of
	{ok, _, _} ->
	    NewT = treap:delete(Key, T),
	    clean_treap(State#state{tab = NewT, size = Size-1});
	_ ->
	    State
    end.

clean_treap(#state{tab = T, size = Size, life_time = LifeTime} = State) ->
    if is_integer(LifeTime) ->
	    Priority = now_priority(),
	    {Cleaned, NewT} = clean_treap(T, Priority + LifeTime, 0),
	    State#state{size = Size - Cleaned, tab = NewT};
       true ->
	    State
    end.

clean_treap(Treap, CleanPriority, N) ->
    case treap:is_empty(Treap) of
        true ->
            {N, Treap};
        false ->
            {_Key, Priority, _Value} = treap:get_root(Treap),
            if Priority > CleanPriority ->
                    clean_treap(treap:delete_root(Treap), CleanPriority, N+1);
	       true ->
                    {N, Treap}
            end
    end.

shrink_treap(#state{tab = T,
		    max_size = MaxSize,
		    shrink_size = ShrinkSize,
		    warn = Warn,
		    size = Size} = State) when Size >= MaxSize ->
    if Warn ->
	    error_logger:warning_msg(
              "shrinking table:~n"
              "** Table: ~p~n"
              "** Processes Number: ~p~n"
              "** Max Size: ~p items~n"
              "** Shrink Size: ~p items~n"
              "** Life Time: ~p microseconds~n"
              "** Hits/Miss: ~p/~p~n"
              "** Owner: ~p~n"
              "** Cache Missed: ~p~n"
              "** Instruction: you have to tune cacheing options"
              " if this message repeats too frequently~n",
              [State#state.name, State#state.procs_num,
               MaxSize, ShrinkSize, State#state.life_time,
               State#state.hits, State#state.miss,
               State#state.owner, State#state.cache_missed]);
       true ->
	    ok
    end,
    {Shrinked, NewT} = shrink_treap(T, ShrinkSize, 0),
    State#state{tab = NewT, size = Size - Shrinked};
shrink_treap(State) ->
    State.

shrink_treap(T, ShrinkSize, ShrinkSize) ->
    {ShrinkSize, T};
shrink_treap(T, ShrinkSize, N) ->
    case treap:is_empty(T) of
	true ->
	    {N, T};
	false ->
	    shrink_treap(treap:delete_root(T), ShrinkSize, N+1)
    end.

print_error(_Operation, _Args, _Reason, #state{warn = false}) ->
    ok;
print_error(Operation, Args, Reason, State) ->
    error_logger:error_msg(
      "callback failed:~n"
      "** Tab: ~p~n"
      "** Owner: ~p~n"
      "** Operation: ~p~n"
      "** Args: ~p~n"
      "** Reason: ~p~n",
      [State#state.name, State#state.owner,
       Operation, Args, Reason]).
