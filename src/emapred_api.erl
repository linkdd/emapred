%%%-------------------------------------------------------------------
%% @doc emapred public API
%% @end
%%%-------------------------------------------------------------------

-module(emapred_api).

%% API
-export([
    run/4
]).

%%====================================================================
%% API
%%====================================================================

run(MapFun, ReducerFun, Acc0, Elements) ->
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
    {ok, Pipeline} = emapred_worker_pool:add(
        emapred_pipeline,
        start_link,
        [Mapper, Reducer]
    ),
    emapred_pipeline:run(Pipeline, Elements).
