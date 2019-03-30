%%%-------------------------------------------------------------------
%% @doc emapred round robin mapper balancer
%% @end
%%%-------------------------------------------------------------------

-module(emapred_mb_round_robin).
-behaviour(emapred_mb).
-behaviour(gen_server).

% Generic Mapper Balancer
-export([
    start_link/1,
    stop/1,
    get/1,
    get_all/1
]).

%% gen_server
-export([
    init/1,
    code_change/3,
    terminate/2,
    handle_call/3,
    handle_cast/2
]).

-record(state, {mappers, cur = 1}).

%%====================================================================
%% Generic Mapper Balancer
%%====================================================================

start_link(Mappers) ->
    gen_server:start_link(?MODULE, Mappers, []).

stop(MapperBalancer) ->
    gen_server:call(MapperBalancer, stop).

get(MapperBalancer) ->
    gen_server:call(MapperBalancer, get).

get_all(MapperBalancer) ->
    gen_server:call(MapperBalancer, get_all).

%%====================================================================
%% Generic Server
%%====================================================================

init(Mappers) ->
    {ok, #state{mappers = Mappers}}.

%%--------------------------------------------------------------------
code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
handle_call(get, _From, State) ->
    CurMapper = lists:nth(State#state.cur, State#state.mappers),
    NextMapper = case State#state.cur < length(State#state.mappers) of
        true  -> CurMapper + 1;
        false -> 1
    end,
    {reply, CurMapper, State#state{cur = NextMapper}};

handle_call(get_all, _From, State) ->
    {reply, State#state.mappers, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
    {reply, {error, {invalid, Request}}, State}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.
