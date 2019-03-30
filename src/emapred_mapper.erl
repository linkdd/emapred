%%%-------------------------------------------------------------------
%% @doc emapred mapper server
%% @end
%%%-------------------------------------------------------------------

-module(emapred_mapper).
-behaviour(gen_server).

%% API
-export([
    start_link/1,
    map/3,
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

-record(state, {func}).

%%====================================================================
%% API
%%====================================================================

start_link(MapFun) ->
    gen_server:start_link(?MODULE, MapFun, []).

%%--------------------------------------------------------------------
map(Mapper, Element, Reducer) ->
    gen_server:call(Mapper, {map, Element, Reducer}).

%%--------------------------------------------------------------------
stop(Mapper) ->
    gen_server:call(Mapper, stop).

%%====================================================================
%% Generic Server
%%====================================================================

init(MapFun) ->
    {ok, #state{func = MapFun}}.

%%--------------------------------------------------------------------
code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
handle_call({map, Element, Reducer}, _From, State) ->
    #state{func = MapFun} = State,
    ok = case MapFun(Element) of
        {emit, {Key, Value}} -> emapred_reducer:emit(Reducer, Key, Value);
        ok                   -> ok
    end,
    {reply, ok, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
    {reply, {error, {invalid, Request}}, State}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.
