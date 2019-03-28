%%%-------------------------------------------------------------------
%% @doc emapred mapper server
%% @end
%%%-------------------------------------------------------------------

-module(emapred_mapper).
-behaviour(gen_server).

%% API
-export([
    start_link/1,
    map/3
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
map(Mapper, Chunk, Reducer) ->
    gen_server:call(Mapper, {map, Chunk, Reducer}).

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
handle_call({map, Chunk, Reducer}, _From, State) ->
    ok = map_chunk(State#state.func, Reducer, Chunk),
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
    {reply, {error, {invalid, Request}}, State}.

%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
    {noreply, State}.

%%====================================================================
%% Internal functions
%%====================================================================

map_chunk(MapFun, Reducer, [Element | Elements]) ->
    ok = case MapFun(Element) of
        {emit, {Key, Value}} -> emapred_reducer:emit(Reducer, Key, Value);
        ok                   -> ok
    end,
    map_chunk(MapFun, Reducer, []);

map_chunk(_MapFun, _Reducer, []) ->
    ok.
