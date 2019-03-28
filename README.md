emapred
=======

Erlang distributed Map-Reduce framework.

Features
--------

 - run arbitrary Map/Reduce functions on a list
 - automatically distribute workload across nodes running this application

Example
-------

    -1 = emapred_api:run(
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
        0,
        % Elements to map
        [5, 6, 3, 4, 7]
    ).

Build
-----

    $ rebar3 compile
