%%%-------------------------------------------------------------------
%% @doc emapred pipeline server
%% @end
%%%-------------------------------------------------------------------

-module(emapred_pipeline).
-behaviour(gen_server).

%% API
-export([
    start_link/2,
    new/3,
    send/2,
    stop/1
]).

%% gen_server
-export([
    init/1,
    code_change/3,
    terminate/2,
    handle_call/3,
    handle_cast/2
]).

-record(state, {mapper, reducer}).

%%====================================================================
%% API
%%====================================================================

start_link(Mapper, Reducer) ->
    gen_server:start_link(?MODULE, {Mapper, Reducer}, []).

%%--------------------------------------------------------------------
new(MapFun, ReducerFun, Acc0) ->
    {ok, Mapper} = emapred_worker_pool:add(
        emapred_mapper,
        start_link,
        [MapFun]
    ),
    {ok, Reducer} = emapred_worker_pool:add(
        emapred_reducer,
        start_link,
        [ReducerFun, Acc0]
    ),
    emapred_worker_pool:add(
        emapred_pipeline,
        start_link,
        [Mapper, Reducer]
    ).

%%--------------------------------------------------------------------
send(Pipeline, Element) ->
    gen_server:call(Pipeline, {send, Element}).

%%--------------------------------------------------------------------
stop(Pipeline) ->
    gen_server:call(Pipeline, stop).

%%====================================================================
%% Generic Server
%%====================================================================

init({Mapper, Reducer}) ->
    {ok, #state{mapper = Mapper, reducer = Reducer}}.

%%--------------------------------------------------------------------
code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
handle_call({send, Element}, _From, State) ->
    ok = emapred_mapper:map(State#state.mapper, Element, State#state.reducer),
    {reply, ok, State};

%%--------------------------------------------------------------------
handle_call(stop, _From, State) ->
    ok = emapred_mapper:stop(State#state.mapper),
    {stop, normal, emapred_reducer:get_value(State#state.reducer), State};

handle_call(Request, _From, State) ->
    {reply, {error, {invalid, Request}}, State}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.
