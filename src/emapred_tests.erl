%%%-------------------------------------------------------------------
%% @doc emapred tests
%% @end
%%%-------------------------------------------------------------------

-module(emapred_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

emapred_pipeline_test() ->
    ok = application:ensure_started(emapred),
    {ok, P} = emapred_pipeline:new(
        % Mapper
        fun(E) ->
            case E > 5 of
                true  -> {emit, {foo, 1}};
                false -> {emit, {foo, -1}}
            end
        end,
        % Reducer
        fun(_Key, Increment, Counter) ->
            {ok, Counter + Increment}
        end,
        % Initial value for reducer accumulator
        0
    ),
    % Stream elements to map
    ok = emapred_pipeline:send(P, 5),
    ok = emapred_pipeline:send(P, 6),
    ok = emapred_pipeline:send(P, 3),
    ok = emapred_pipeline:send(P, 4),
    ok = emapred_pipeline:send(P, 7),
    % Stop streaming and get reduced result
    ?assertEqual(-1, emapred_pipeline:stop(P)).

-endif.