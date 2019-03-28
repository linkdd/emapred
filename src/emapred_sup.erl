%%%-------------------------------------------------------------------
%% @doc emapred top level supervisor
%% @end
%%%-------------------------------------------------------------------

-module(emapred_sup).
-behaviour(supervisor).

%% API
-export([start_link/0, init/1]).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%====================================================================
%% Supervisor
%%====================================================================

init([]) ->
    {ok, {{one_for_one, 1, 5}, [
        #{
            id => emapred_pool,
            start => {emapred_worker_pool, start_link, []},
            restart => permanent,
            type => supervisor,
            modules => [emapred_worker_pool]
        }
    ]}}.
