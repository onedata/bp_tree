%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains bp_tree_children module tests.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_children_test).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").
-include_lib("eunit/include/eunit.hrl").


-define(Val, ?Val(1)).
-define(Val(N), <<"value-", (integer_to_binary(N))/binary>>).


bp_tree_children_test_() ->
    {foreach,
        fun() ->
            _MkKeyFun = case rand:uniform(3) of
                1 -> fun(N) -> sortkey:pack_lexicographic(<<"key-", (integer_to_binary(N))/binary>>) end;
                2 -> fun(N) -> sortkey:pack_numeric(N) end;
                3 -> fun(N) -> sortkey:pack_compound([<<"key">>, N, <<"suffix">>]) end
            end
        end,
        fun(_) -> ok end,
        [
            fun insert_should_succeed/1,
            fun insert_should_return_already_exists_error/1,
            fun remove_should_return_empty_error/1,
            fun remove_should_return_not_found_error/1,
            fun remove_should_succeed/1,
            fun size_should_succeed/1,
            fun to_list_should_succeed/1,
            fun from_list_should_succeed/1,
            fun to_map_should_succeed/1,
            fun from_map_should_succeed/1,
            fun insert_should_maintain_order/1,
            fun accessor_should_succeed/1,
            fun accessor_should_return_out_of_range_error/1,
            fun find_should_succeed/1,
            fun find_should_return_not_found_error/1,
            fun lower_bound_should_succeed/1
        ]
    }.



insert_should_succeed(MkKey) ->
    A = bp_tree_children:new(128),
    Key = MkKey(1),
    [
        ?_assertMatch({ok, _, [Key]}, bp_tree_children:insert({left, [{Key, ?Val}]}, A, 100)),
        ?_assertMatch({ok, _, [Key]}, bp_tree_children:insert({key, [{Key, ?Val}]}, A, 100)),
        ?_assertMatch({ok, _, [Key]}, bp_tree_children:insert({right, [{Key, ?Val}]}, A, 100)),
        ?_assertMatch({ok, _, [Key]}, bp_tree_children:insert({both, [{Key, {?Val, ?Val}}]}, A, 100))
    ].


insert_should_return_already_exists_error(MkKey) ->
    A = bp_tree_children:new(128),
    Key = MkKey(1),
    {ok, A2, [Key]} = bp_tree_children:insert({left, [{Key, ?Val}]}, A, 100),
    ?_assertEqual({error, already_exists}, bp_tree_children:insert({left, [{Key, ?Val}]}, A2, 100)).


insert_should_maintain_order(MkKey) ->
    {foreachx,
        fun(Keys) ->
            A = lists:foldl(fun(Key, A2) ->
                {ok, A3, [Key]} = bp_tree_children:insert({both, [{Key, {?Val, ?Val}}]}, A2, 100),
                A3
            end, bp_tree_children:new(128), Keys),
            Keys2 = lists:reverse(lists:sort(Keys)),
            List = lists:foldl(fun(Key, Acc) ->
                [?Val, Key | Acc]
            end, [?Val], Keys2),
            {A, List}
        end,
        fun(_, _) -> ok end,
        [
            {[MkKey(1), MkKey(2), MkKey(3), MkKey(4), MkKey(5)], fun(_, {A, List}) ->
                {"ordered", ?_assertEqual(List, bp_tree_children:to_list(A))}
            end},
            {[MkKey(5), MkKey(4), MkKey(3), MkKey(2), MkKey(1)], fun(_, {A, List}) ->
                {"reversed", ?_assertEqual(List, bp_tree_children:to_list(A))}
            end},
            {[MkKey(3), MkKey(1), MkKey(5), MkKey(4), MkKey(2)], fun(_, {A, List}) ->
                {"random", ?_assertEqual(List, bp_tree_children:to_list(A))}
            end}
        ]
    }.


remove_should_return_empty_error(MkKey) ->
    A = bp_tree_children:new(128),
    ?_assertEqual({error, not_found}, bp_tree_children:remove({left,
        [{MkKey(1), fun(_) -> true end}]}, A)).


remove_should_return_not_found_error(MkKey) ->
    A = bp_tree_children:from_list([?Val, MkKey(1), ?Val, MkKey(3), ?Val]),
    ?_assertEqual({error, not_found}, bp_tree_children:remove({left,
        [{MkKey(2), fun(_) -> true end}]}, A)).


remove_should_succeed(MkKey) ->
    K1 = MkKey(1),
    K3 = MkKey(3),
    K5 = MkKey(5),
    A = bp_tree_children:from_list([?Val, K1, ?Val, K3, ?Val, K5, ?Val]),
    A2 = bp_tree_children:from_list([?Val, K1, ?Val, K5, ?Val, ?NIL, ?NIL]),
    A3 = bp_tree_children:from_list([?Val, K5, ?Val, ?NIL, ?NIL, ?NIL, ?NIL]),
    A4 = bp_tree_children:from_list([?Val, ?NIL, ?NIL, ?NIL, ?NIL, ?NIL, ?NIL]),
    [
        ?_assertEqual({ok, A2, [K3]}, bp_tree_children:remove({left, [{K3, fun(_) -> true end}]}, A)),
        ?_assertEqual({ok, A3, [K1]}, bp_tree_children:remove({left, [{K1, fun(_) -> true end}]}, A2)),
        ?_assertEqual({ok, A4, [K5]}, bp_tree_children:remove({left, [{K5, fun(_) -> true end}]}, A3))
    ].


accessor_should_succeed(MkKey) ->
    {setup,
        fun() -> bp_tree_children:from_list([1, MkKey(2), 3, MkKey(4), 5]) end,
        fun(_) -> ok end,
        {with, [
            fun(A) ->
                ?assertEqual({ok, 1}, bp_tree_children:get_value({left, 1}, A)),
                ?assertEqual({ok, MkKey(2)}, bp_tree_children:get_key(1, A)),
                ?assertEqual({ok, 3}, bp_tree_children:get_value({right, 1}, A))
            end,
            fun(A) ->
                ?assertEqual({ok, 3}, bp_tree_children:get_value({left, 2}, A)),
                ?assertEqual({ok, MkKey(4)}, bp_tree_children:get_key(2, A)),
                ?assertEqual({ok, 5}, bp_tree_children:get_value({right, 2}, A))
            end
        ]}
    }.


accessor_should_return_out_of_range_error(MkKey) ->
    {setup,
        fun() -> bp_tree_children:from_list([?Val, MkKey(2), ?Val, MkKey(4), ?Val]) end,
        fun(_) -> ok end,
        {with, [
            fun(A) ->
                ?assertEqual({error, out_of_range}, bp_tree_children:get_value({left, 0}, A)),
                ?assertEqual({error, out_of_range}, bp_tree_children:get_key(0, A)),
                ?assertEqual({ok, ?Val}, bp_tree_children:get_value({right, 0}, A))
            end,
            fun(A) ->
                ?assertEqual({error, out_of_range}, bp_tree_children:get_value({left, 3}, A)),
                ?assertEqual({error, out_of_range}, bp_tree_children:get_key(3, A)),
                ?assertEqual({error, out_of_range}, bp_tree_children:get_value({right, 3}, A))
            end,
            fun(A) ->
                ?assertEqual({error, out_of_range}, bp_tree_children:get_value({left, 5}, A)),
                ?assertEqual({error, out_of_range}, bp_tree_children:get_key(5, A)),
                ?assertEqual({error, out_of_range}, bp_tree_children:get_value({right, 5}, A))
            end
        ]}
    }.


find_should_succeed(MkKey) ->
    {setup,
        fun() ->
            bp_tree_children:from_list([?Val, MkKey(1), ?Val, MkKey(3), ?Val, MkKey(5), ?Val])
        end,
        fun(_) -> ok end,
        {with, [
            fun(M) -> ?assertEqual({ok, 1}, bp_tree_children:find(MkKey(1), M)) end,
            fun(M) -> ?assertEqual({ok, 2}, bp_tree_children:find(MkKey(3), M)) end,
            fun(M) -> ?assertEqual({ok, 3}, bp_tree_children:find(MkKey(5), M)) end
        ]}
    }.


find_should_return_not_found_error(MkKey) ->
    {setup,
        fun() ->
            A = bp_tree_children:from_list([?Val, MkKey(1), ?Val, MkKey(3), ?Val, MkKey(5), ?Val]),
            {A, {error, not_found}}
        end,
        fun(_) -> ok end,
        {with, [
            fun({A, Err}) ->
                ?assertEqual(Err, bp_tree_children:find(MkKey(0), A)) end,
            fun({A, Err}) ->
                ?assertEqual(Err, bp_tree_children:find(MkKey(2), A)) end,
            fun({A, Err}) ->
                ?assertEqual(Err, bp_tree_children:find(MkKey(4), A)) end,
            fun({A, Err}) ->
                ?assertEqual(Err, bp_tree_children:find(MkKey(6), A))
            end
        ]}
    }.


lower_bound_should_succeed(MkKey) ->
    {setup,
        fun() ->
            bp_tree_children:from_list([?Val, MkKey(1), ?Val, MkKey(3), ?Val, MkKey(5), ?Val])
        end,
        fun(_) -> ok end,
        {with, [
            fun(A) -> ?assertEqual(1, bp_tree_children:lower_bound(MkKey(0), A)) end,
            fun(A) -> ?assertEqual(1, bp_tree_children:lower_bound(MkKey(1), A)) end,
            fun(A) -> ?assertEqual(2, bp_tree_children:lower_bound(MkKey(2), A)) end,
            fun(A) -> ?assertEqual(2, bp_tree_children:lower_bound(MkKey(3), A)) end,
            fun(A) -> ?assertEqual(3, bp_tree_children:lower_bound(MkKey(4), A)) end,
            fun(A) -> ?assertEqual(3, bp_tree_children:lower_bound(MkKey(5), A)) end,
            fun(A) -> ?assertEqual(4, bp_tree_children:lower_bound(MkKey(6), A)) end
        ]}
    }.


size_should_succeed(MkKey) ->
    A1 = bp_tree_children:new(128),
    K1 = MkKey(1),
    {ok, A2, [K1]} = bp_tree_children:insert({left, [{K1, ?Val}]}, A1, 100),
    K2 = MkKey(2),
    {ok, A3, [K2]} = bp_tree_children:insert({left, [{K2, ?Val}]}, A2, 100),
    [
        ?_assertEqual(0, bp_tree_children:size(A1)),
        ?_assertEqual(1, bp_tree_children:size(A2)),
        ?_assertEqual(2, bp_tree_children:size(A3))
    ].


to_list_should_succeed(MkKey) ->
    A1 = bp_tree_children:new(128),
    K1 = MkKey(1),
    {ok, A2, [K1]} = bp_tree_children:insert({left, [{K1, ?Val}]}, A1, 100),
    K2 = MkKey(2),
    {ok, A3, [K2]} = bp_tree_children:insert({left, [{K2, ?Val}]}, A2, 100),
    ?_assertEqual([?Val, MkKey(1), ?Val, MkKey(2)], bp_tree_children:to_list(A3)).


from_list_should_succeed(MkKey) ->
    A1 = bp_tree_children:new(128),
    K1 = MkKey(1),
    {ok, A2, [K1]} = bp_tree_children:insert({left, [{K1, ?Val}]}, A1, 100),
    K2 = MkKey(2),
    {ok, A3, [K2]} = bp_tree_children:insert({left, [{K2, ?Val}]}, A2, 100),
    ?_assertEqual(bp_tree_children:to_list(A3), bp_tree_children:to_list(
        bp_tree_children:from_list([?Val, MkKey(1), ?Val, MkKey(2)]))).


to_map_should_succeed(MkKey) ->
    A1 = bp_tree_children:new(128),
    K1 = MkKey(1),
    {ok, A2, [K1]} = bp_tree_children:insert({left, [{K1, ?Val(1)}]}, A1, 100),
    K2 = MkKey(2),
    {ok, A3, [K2]} = bp_tree_children:insert({left, [{K2, ?Val(2)}]}, A2, 100),
    {ok, A4} = bp_tree_children:update_last_value(?Val(3), A3),
    [
        ?_assertEqual(#{
            ?SIZE_KEY => 257
        }, bp_tree_children:to_map(A1)),
        ?_assertEqual(#{
            ?SIZE_KEY => 257,
            MkKey(1) => ?Val(1)
        }, bp_tree_children:to_map(A2)),
        ?_assertEqual(#{
            ?SIZE_KEY => 257,
            MkKey(1) => ?Val(1),
            MkKey(2) => ?Val(2)
        }, bp_tree_children:to_map(A3)),
        ?_assertEqual(#{
            ?SIZE_KEY => 257,
            MkKey(1) => ?Val(1),
            MkKey(2) => ?Val(2),
            ?LAST_KEY => ?Val(3)
        }, bp_tree_children:to_map(A4))
    ].


from_map_should_succeed(MkKey) ->
    A1 = bp_tree_children:new(128),
    K1 = MkKey(1),
    {ok, A2, [K1]} = bp_tree_children:insert({left, [{K1, ?Val(1)}]}, A1, 100),
    K2 = MkKey(2),
    {ok, A3, [K2]} = bp_tree_children:insert({left, [{K2, ?Val(2)}]}, A2, 100),
    {ok, A4} = bp_tree_children:update_last_value(?Val(3), A3),
    [
        ?_assertEqual(A1, bp_tree_children:from_map(#{
            ?SIZE_KEY => 257
        })),
        ?_assertEqual(A2, bp_tree_children:from_map(#{
            ?SIZE_KEY => 257,
            MkKey(1) => ?Val(1)
        })),
        ?_assertEqual(bp_tree_children:to_list(A3), bp_tree_children:to_list(
            bp_tree_children:from_map(#{
                ?SIZE_KEY => 257,
                MkKey(2) => ?Val(2),
                MkKey(1) => ?Val(1)
            }))),
        ?_assertEqual(bp_tree_children:to_list(A4), bp_tree_children:to_list(
            bp_tree_children:from_map(#{
                ?SIZE_KEY => 257,
                ?LAST_KEY => ?Val(3),
                MkKey(2) => ?Val(2),
                MkKey(1) => ?Val(1)
            })
        ))
    ].
