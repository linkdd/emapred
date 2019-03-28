%%%-------------------------------------------------------------------
%% @doc emapred pipeline server
%% @end
%%%-------------------------------------------------------------------

-module(emapred_pipeline).
-behaviour(gen_server).

%% API
-export([
    start_link/2,
    run/2
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
run(Pipeline, Elements) ->
    gen_server:call(Pipeline, {run, Elements}).

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
handle_call({run, Elements}, _From, State) ->
    {stop, normal, run_pipeline(State, Elements), State};

handle_call(Request, _From, State) ->
    {reply, {error, {invalid, Request}}, State}.

%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%====================================================================
%% Internal functions
%%====================================================================

run_pipeline(#state{mapper = Mapper, reducer = Reducer}, Elements) ->
    ok = emapred_mapper:map(Mapper, Elements, Reducer),
    emapred_reducer:get_value(Reducer).
