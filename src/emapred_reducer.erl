%%%-------------------------------------------------------------------
%% @doc emapred reducer server
%% @end
%%%-------------------------------------------------------------------

-module(emapred_reducer).
-behaviour(gen_server).

%% API
-export([
    start_link/2,
    emit/3,
    get_value/1
]).

%% gen_server
-export([
    init/1,
    code_change/3,
    terminate/2,
    handle_call/3,
    handle_cast/2
]).

-record(state, {func, acc}).

%%====================================================================
%% API
%%====================================================================

start_link(ReducerFun, Acc0) ->
    gen_server:start_link(?MODULE, {ReducerFun, Acc0}, []).

%%--------------------------------------------------------------------
emit(Reducer, Key, Value) ->
    gen_server:call(Reducer, {reduce, Key, Value}).

%%--------------------------------------------------------------------
get_value(Reducer) ->
    gen_server:call(Reducer, get_value).

%%====================================================================
%% Generic Server
%%====================================================================

init({ReducerFun, Acc0}) ->
    {ok, #state{func = ReducerFun, acc = Acc0}}.

%%--------------------------------------------------------------------
code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
handle_call({reduce, Key, Value}, _From, State) ->
    {ok, NewState} = reduce(State, Key, Value),
    {reply, ok, NewState};

handle_call(get_value, _From, State) ->
    {stop, normal, State#state.acc, State};

handle_call(Request, _From, State) ->
    {reply, {error, {invalid, Request}}, State}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%====================================================================
%% Internal functions
%%====================================================================

reduce(#state{func = Fun, acc = Acc}, Key, Value) ->
    {ok, NewAcc} = Fun(Key, Value, Acc),
    {ok, #state{func = Fun, acc = NewAcc}}.
