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
    new/4,
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

-record(state, {reducer, mb}).

%%====================================================================
%% API
%%====================================================================

start_link(Reducer, MBModule) ->
    gen_server:start_link(?MODULE, {Reducer, MBModule}, []).

%%--------------------------------------------------------------------
new(MapFun, ReducerFun, Acc0) ->
    new(MapFun, ReducerFun, Acc0, []).

%%--------------------------------------------------------------------
new(MapFun, ReducerFun, Acc0, Options) ->
    Mappers = [
        Mapper || {ok, Mapper} <- [
            emapred_worker_pool:add(
                emapred_mapper,
                start_link,
                [MapFun]
            ) || _ <- lists:seq(1, proplists:get_value(mapper_count, Options, 1))
        ]
    ],
    {ok, Reducer} = emapred_worker_pool:add(
        emapred_reducer,
        start_link,
        [ReducerFun, Acc0]
    ),
    MBModule = proplists:get_value(mapper_balancer, Options, emapred_mb_round_robin),
    {ok, MBPid} = emapred_worker_pool:add(
        MBModule,
        start_link,
        [Mappers]
    ),
    emapred_worker_pool:add(
        emapred_pipeline,
        start_link,
        [Reducer, {MBModule, MBPid}]
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

init({Reducer, MapperBalancer}) ->
    {ok, #state{reducer = Reducer, mb = MapperBalancer}}.

%%--------------------------------------------------------------------
code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
terminate(_Reason, #state{mb = {MBModule, MBPid}}) ->
    MBModule:stop(MBPid).

%%--------------------------------------------------------------------
handle_call({send, Element}, _From, State = #state{reducer = Reducer, mb = {MBModule, MBPid}}) ->
    ok = emapred_mapper:map(MBModule:get(MBPid), Element, Reducer),
    {reply, ok, State};

%%--------------------------------------------------------------------
handle_call(stop, _From, State = #state{reducer = Reducer, mb = {MBModule, MBPid}}) ->
    lists:foreach(
        fun(Mapper) ->
            ok = emapred_mapper:stop(Mapper),
            ok
        end,
        MBModule:get_all(MBPid)
    ),
    {stop, normal, emapred_reducer:get_value(Reducer), State};

handle_call(Request, _From, State) ->
    {reply, {error, {invalid, Request}}, State}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.
