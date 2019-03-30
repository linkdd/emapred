%%%-------------------------------------------------------------------
%% @doc emapred generic mapper balancer
%% @end
%%%-------------------------------------------------------------------

-module(emapred_mb).

-callback start_link(Mappers :: list()) -> {ok, pid()}.
-callback stop(MapperBalancer :: pid()) -> ok.
-callback get(MapperBalancer :: pid()) -> any().
-callback get_all(MapperBalancer :: pid) -> list().
