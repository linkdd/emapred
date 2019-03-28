%%%-------------------------------------------------------------------
%% @doc emapred load balancer
%% @end
%%%-------------------------------------------------------------------

-module(emapred_lb).

%% API
-export([
    get_best_node/0
]).

%%====================================================================
%% API
%%====================================================================

get_best_node() ->
    get_best_node([node() | nodes()], undefined).

%%====================================================================
%% Internal functions
%%====================================================================

get_best_node([Node | Nodes], undefined) ->
    case check_application_presence(Node) of
        present -> get_best_node(Nodes, {Node, emapred_worker_pool:count_workers(Node)});
        absent  -> get_best_node(Nodes, undefined)
    end;

get_best_node([Node | Nodes], {BestNode, BestNodeCount}) ->
    case check_application_presence(Node) of
        present ->
            Count = emapred_worker_pool:count_workers(Node),
            NextBestNodeInfo = case Count < BestNodeCount of
                true  -> {Node, Count};
                false -> {BestNode, BestNodeCount}
            end,
            get_best_node(Nodes, NextBestNodeInfo);
        absent ->
            get_best_node(Nodes, {BestNode, BestNodeCount})
    end;

get_best_node([], undefined) ->
    undefined;

get_best_node([], {BestNode, _BestNodeCount}) ->
    BestNode.

%%--------------------------------------------------------------------
check_application_presence(Node) ->
    case rpc:call(Node, application, get_application, emapred_app) of
        {ok, emapred} -> present;
        _             -> absent
    end.
