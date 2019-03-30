emapred
=======

Erlang distributed Map-Reduce framework.

Features
--------

 - run arbitrary Map/Reduce functions on a list
 - automatically distribute workload across nodes running this application

Example
-------

.. code-block:: erlang

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
    -1 = emapred_pipeline:stop(P).

Build
-----

    $ rebar3 compile
