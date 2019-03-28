%%%-------------------------------------------------------------------
%% @doc emapred worker pool supervisor
%% @end
%%%-------------------------------------------------------------------

-module(emapred_worker_pool).
-behaviour(supervisor).

%% API
-export([
    start_link/0,
    add/3,
    count_workers/1,
    init/1
]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%--------------------------------------------------------------------
add(M, F, A) ->
    Node = emapred_lb:get_best_node(),
    supervisor:start_child({?SERVER, Node}, [M, F, A]).

%%--------------------------------------------------------------------
count_workers(Node) ->
    Counts = supervisor:count_children({?SERVER, Node}),
    proplists:get_value(active, Counts, 0).

%%====================================================================
%% Supervisor
%%====================================================================

init([]) ->
    {ok, {{simple_one_for_one, 0, 1}, [
        #{
            id => emapred_worker_instance,
            start => {erlang, apply, []},
            restart => temporary,
            type => worker
        }
    ]}}.
