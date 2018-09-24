%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides B+ tree node management functions.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_node).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([new/2]).
-export([key/2, value/2, size/1]).
-export([right_sibling/1, set_right_sibling/2]).
-export([child/2, child_with_sibling/2, child_with_right_sibling/2]).
-export([leftmost_child/1]).
-export([find/2, find_pos/2, lower_bound/2, left_sibling/2]).
-export([insert/3, remove/2, merge/3, split/1]).
-export([rotate_right/4, rotate_left/4]).
-export([fold/4]).
-export([get_rebalance_info/1, set_rebalance_info/2]).
-export([get_order/1, set_order/2]).

-type id() :: any().
-type rebalance_info() :: undefined |
    #{links_tree:id() => {bp_tree_node:id(), bp_tree:key()}}.
-export_type([id/0, rebalance_info/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates B+ tree node.
%% @end
%%--------------------------------------------------------------------
-spec new(boolean(), bp_tree:order()) -> bp_tree:tree_node().
new(Leaf, MaxSize) ->
    #bp_tree_node{
        leaf = Leaf,
        children = bp_tree_children:new(2 * MaxSize + 1)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns key at a position or fails with a out of range error.
%% @end
%%--------------------------------------------------------------------
-spec key(pos_integer(), bp_tree:tree_node()) ->
    {ok, bp_tree:value()} | {error, out_of_range}.
key(Pos, #bp_tree_node{leaf = true, children = Children}) ->
    bp_tree_children:get({key, Pos}, Children).

%%--------------------------------------------------------------------
%% @doc
%% Returns value at a position or fails with a out of range error.
%% @end
%%--------------------------------------------------------------------
-spec value(pos_integer(), bp_tree:tree_node()) ->
    {ok, bp_tree:value()} | {error, out_of_range}.
value(Pos, #bp_tree_node{leaf = true, children = Children}) ->
    bp_tree_children:get({left, Pos}, Children).

%%--------------------------------------------------------------------
%% @doc
%% Returns number of node's children.
%% @end
%%--------------------------------------------------------------------
-spec size(bp_tree:tree_node()) -> non_neg_integer().
size(#bp_tree_node{children = Children}) ->
    bp_tree_children:size(Children).

%%--------------------------------------------------------------------
%% @doc
%% Returns child node on path from a root to a leaf associated with a key.
%% @end
%%--------------------------------------------------------------------
-spec child(bp_tree:key(), bp_tree:tree_node()) ->
    {ok, id()} | {error, not_found}.
child(_Key, #bp_tree_node{leaf = true}) ->
    {error, not_found};
child(Key, #bp_tree_node{leaf = false, children = Children}) ->
    case bp_tree_children:get({lower_bound, Key}, Children) of
        {ok, NodeId} ->
            {ok, NodeId};
        {error, out_of_range} ->
            {ok, _NodeId} = bp_tree_children:get({right, last}, Children)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns child node with sibling on path from a root to a leaf associated with
%% a key. Prefers left child to the right child.
%% @end
%%--------------------------------------------------------------------
-spec child_with_sibling(bp_tree:key(), bp_tree:tree_node()) ->
    {ok, id(), bp_tree:key(), id()} | {error, not_found}.
child_with_sibling(_Key, #bp_tree_node{leaf = true}) ->
    {error, not_found};
child_with_sibling(Key, #bp_tree_node{leaf = false, children = Children}) ->
    Pos = bp_tree_children:lower_bound(Key, Children),
    case bp_tree_children:get({left, Pos}, Children) of
        {ok, NodeId} ->
            case bp_tree_children:get({left, Pos - 1}, Children) of
                {ok, LNodeId} ->
                    {ok, Key2} = bp_tree_children:get({key, Pos - 1}, Children),
                    {ok, LNodeId, Key2, NodeId};
                {error, out_of_range} ->
                    {ok, Key2} = bp_tree_children:get({key, Pos}, Children),
                    {ok, RNodeId} = bp_tree_children:get({right, Pos}, Children),
                    {ok, NodeId, Key2, RNodeId}
            end;
        {error, out_of_range} ->
            {ok, Key2} = bp_tree_children:get({key, last}, Children),
            {ok, {LNodeId, RNodeId}} = bp_tree_children:get({both, last}, Children),
            {ok, LNodeId, Key2, RNodeId}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns child node with right sibling on path from a root to a leaf
%% associated with a key.
%% @end
%%--------------------------------------------------------------------
-spec child_with_right_sibling(bp_tree:key(), bp_tree:tree_node()) ->
    {ok, bp_tree_node:id(), bp_tree_node:id()} | {error, not_found}.
child_with_right_sibling(_Key, #bp_tree_node{leaf = true}) ->
    {error, not_found};
child_with_right_sibling(Key, #bp_tree_node{leaf = false, children = Children}) ->
    Pos = bp_tree_children:lower_bound(Key, Children),
    case bp_tree_children:get({left, Pos}, Children) of
        {ok, NodeId} ->
            {ok, RNodeId} = bp_tree_children:get({right, Pos}, Children),
            {ok, NodeId, RNodeId};
        {error, out_of_range} ->
            {ok, NodeId} = bp_tree_children:get({right, last}, Children),
            {ok, NodeId, ?NIL}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns left sibling on path from a root to a leaf
%% associated with a key.
%% @end
%%--------------------------------------------------------------------
-spec left_sibling(bp_tree:key(), bp_tree:tree_node()) ->
    {ok, bp_tree_node:id()} | {error, out_of_range}.
left_sibling(Key, #bp_tree_node{leaf = false, children = Children}) ->
    Pos = bp_tree_children:lower_bound(Key, Children) - 2,
    bp_tree_children:get({left, Pos}, Children).

%%--------------------------------------------------------------------
%% @doc
%% Returns leftmost child node on path from a root to a leaf.
%% @end
%%--------------------------------------------------------------------
-spec leftmost_child(bp_tree:tree_node()) ->
    {ok, id()} | {error, not_found}.
leftmost_child(#bp_tree_node{leaf = true}) ->
    {error, not_found};
leftmost_child(#bp_tree_node{leaf = false, children = Children}) ->
    {ok, _NodeId} = bp_tree_children:get({left, first}, Children).

%%--------------------------------------------------------------------
%% @doc
%% Returns right sibling of a leaf node.
%% @end
%%--------------------------------------------------------------------
-spec right_sibling(bp_tree:tree_node()) ->
    {ok, id()} | {error, not_found}.
right_sibling(#bp_tree_node{leaf = true, children = Children}) ->
    case bp_tree_children:get({right, last}, Children) of
        {ok, ?NIL} -> {error, not_found};
        {ok, NodeId} -> {ok, NodeId};
        {error, out_of_range} -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% In case of a leaf sets the ID of its right sibling, otherwise does nothing.
%% @end
%%--------------------------------------------------------------------
-spec set_right_sibling(id(), bp_tree:tree_node()) -> bp_tree:tree_node().
set_right_sibling(NodeId, Node = #bp_tree_node{
    leaf = true, children = Children
}) ->
    {ok, Children2} = bp_tree_children:update_last_value(NodeId, Children),
    Node#bp_tree_node{children = Children2};
set_right_sibling(_NodeId, Node = #bp_tree_node{leaf = false}) ->
    Node.

%%--------------------------------------------------------------------
%% @doc
%% Returns value associated with a key from leaf node or fails with a missing
%% error.
%% @end
%%--------------------------------------------------------------------
-spec find(bp_tree:key(), bp_tree:tree_node()) ->
    {ok, bp_tree:value()} | {error, not_found}.
find(Key, #bp_tree_node{leaf = true, children = Children}) ->
    bp_tree_children:find_value(Key, Children).

%%--------------------------------------------------------------------
%% @doc
%% Returns position of a key in leaf node or fails with a missing error.
%% @end
%%--------------------------------------------------------------------
-spec find_pos(bp_tree:key(), bp_tree:tree_node()) ->
    {ok, pos_integer()} | {error, not_found}.
find_pos(Key, #bp_tree_node{leaf = true, children = Children}) ->
    bp_tree_children:find(Key, Children).

%%--------------------------------------------------------------------
%% @doc
%% Returns position of a first key that does not compare less than a key.
%% @end
%%--------------------------------------------------------------------
-spec lower_bound(bp_tree:key(), bp_tree:tree_node()) -> pos_integer().
lower_bound(Key, #bp_tree_node{leaf = true, children = Children}) ->
    bp_tree_children:lower_bound(Key, Children).

%%--------------------------------------------------------------------
%% @doc
%% Inserts key-value pair into a node.
%% @end
%%--------------------------------------------------------------------
-spec insert(bp_tree:key(), bp_tree:value(), bp_tree:tree_node()) ->
    {ok, bp_tree:tree_node()} | {error, term()}.
insert(Key, Value, Node = #bp_tree_node{leaf = true, children = Children}) ->
    case bp_tree_children:insert({left, Key}, Value, Children) of
        {ok, Children2} -> {ok, Node#bp_tree_node{children = Children2}};
        {error, Reason} -> {error, Reason}
    end;
insert(Key, Value, Node = #bp_tree_node{leaf = false, children = Children}) ->
    case bp_tree_children:insert({both, Key}, Value, Children) of
        {ok, Children2} -> {ok, Node#bp_tree_node{children = Children2}};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes key and associated value from a node.
%% @end
%%--------------------------------------------------------------------
%%-spec remove(bp_tree:key(), bp_tree:remove_pred(), bp_tree:tree_node()) ->
%%    {ok, bp_tree:tree_node()} | {error, term()}.
remove(Items, Node = #bp_tree_node{leaf = true, children = Children}) ->
    case bp_tree_children:remove({left, Items}, Children) of
        {ok, Children2, RemovedKeys} ->
            {ok, Node#bp_tree_node{children = Children2}, RemovedKeys};
        {error, Reason} ->
            {error, Reason}
    end;
remove(Items, Node = #bp_tree_node{leaf = false, children = Children}) ->
    case bp_tree_children:remove({right, Items}, Children) of
        {ok, Children2, RemovedKeys} ->
            {ok, Node#bp_tree_node{children = Children2}, RemovedKeys};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Merges two nodes into a single node.
%% @end
%%--------------------------------------------------------------------
-spec merge(bp_tree:tree_node(), bp_tree:key(), bp_tree:tree_node()) ->
    bp_tree:tree_node().
merge(Node = #bp_tree_node{leaf = true, children = LChildren}, _ParentKey,
    #bp_tree_node{leaf = true, children = RChildren}) ->
    LChildren2 = bp_tree_children:merge(LChildren, RChildren),
    Node#bp_tree_node{children = LChildren2};
merge(Node = #bp_tree_node{leaf = false, children = LChildren}, ParentKey,
    #bp_tree_node{leaf = false, children = RChildren}) ->
    {ok, LChildren2} = bp_tree_children:append({key, ParentKey}, ParentKey, LChildren),
    LChildren3 = bp_tree_children:merge(LChildren2, RChildren),
    Node#bp_tree_node{children = LChildren3}.

%%--------------------------------------------------------------------
%% @doc
%% Splits node in half. Returns updated original node, newly created one
%% and a key that should be inserted into parent node.
%% @end
%%--------------------------------------------------------------------
-spec split(bp_tree:tree_node()) ->
    {ok, bp_tree:tree_node(), bp_tree:key(), bp_tree:tree_node()}.
split(LNode = #bp_tree_node{leaf = true, children = Children}) ->
    {LChildren, Key, RChildren} = bp_tree_children:split(Children),
    {ok, LChildren2} = bp_tree_children:append({key, Key}, Key, LChildren),
    RNode = #bp_tree_node{leaf = true, children = RChildren},
    {ok, LNode#bp_tree_node{children = LChildren2}, Key, RNode};
split(LNode = #bp_tree_node{leaf = false, children = Children}) ->
    {LChildren, Key, RChildren} = bp_tree_children:split(Children),
    RNode = #bp_tree_node{leaf = false, children = RChildren},
    {ok, LNode#bp_tree_node{children = LChildren}, Key, RNode}.

%%--------------------------------------------------------------------
%% @doc
%% Moves values from a left sibling to a node until size of node is less than
%% order.
%% @end
%%--------------------------------------------------------------------
-spec rotate_right(bp_tree:tree_node(), bp_tree:key(), bp_tree:tree_node(),
    non_neg_integer()) ->
    {bp_tree:tree_node(), bp_tree:key(), bp_tree:tree_node()}.
rotate_right(LNode, ParentKey, RNode, Order) ->
    {LNode2, ParentKey2, RNode2} = Ans =
        rotate_right(LNode, ParentKey, RNode),
    case ?MODULE:size(RNode2) < Order of
        true ->
            rotate_right(LNode2, ParentKey2, RNode2, Order);
        _ ->
            Ans
    end.

%%--------------------------------------------------------------------
%% @doc
%% Moves values from a right sibling to a node until size of node is less than
%% order.
%% @end
%%--------------------------------------------------------------------
-spec rotate_left(bp_tree:tree_node(), bp_tree:key(), bp_tree:tree_node(),
    non_neg_integer()) ->
    {bp_tree:tree_node(), bp_tree:key(), bp_tree:tree_node()}.
rotate_left(LNode, ParentKey, RNode, Order) ->
    {LNode2, ParentKey2, RNode2} = Ans =
        rotate_left(LNode, ParentKey, RNode),
    case ?MODULE:size(LNode2) < Order of
        true ->
            rotate_left(LNode2, ParentKey2, RNode2, Order);
        _ ->
            Ans
    end.

%%--------------------------------------------------------------------
%% @doc
%% Folds B+ tree node.
%% @end
%%--------------------------------------------------------------------
-spec fold(bp_tree:fold_start_spec(), bp_tree:tree_node(),
    bp_tree:fold_fun(), bp_tree:fold_acc()) -> bp_tree:fold_acc().
fold(KeyOrPos, #bp_tree_node{children = LChildren}, Fun, Acc) ->
    bp_tree_children:fold(KeyOrPos, LChildren, Fun, Acc).

%%--------------------------------------------------------------------
%% @doc
%% Gets rebalance_info field.
%% @end
%%--------------------------------------------------------------------
-spec get_rebalance_info(bp_tree:tree_node()) -> rebalance_info().
get_rebalance_info(#bp_tree_node{rebalance_info = Info}) ->
    Info.

%%--------------------------------------------------------------------
%% @doc
%% Sets rebalance_info field.
%% @end
%%--------------------------------------------------------------------
-spec set_rebalance_info(bp_tree:tree_node(), rebalance_info()) ->
    bp_tree:tree_node().
set_rebalance_info(Node, Info) ->
    Node#bp_tree_node{rebalance_info = Info}.

%%--------------------------------------------------------------------
%% @doc
%% Gets order field.
%% @end
%%--------------------------------------------------------------------
-spec get_order(bp_tree:tree_node()) -> undefined | non_neg_integer().
get_order(#bp_tree_node{order = Order}) ->
    Order.

%%--------------------------------------------------------------------
%% @doc
%% Sets order field.
%% @end
%%--------------------------------------------------------------------
-spec set_order(bp_tree:tree_node(), undefined | non_neg_integer()) ->
    bp_tree:tree_node().
set_order(Node, Order) ->
    Node#bp_tree_node{order = Order}.

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Moves maximum value from a left sibling to a node.
%% @end
%%--------------------------------------------------------------------
-spec rotate_right(bp_tree:tree_node(), bp_tree:key(), bp_tree:tree_node()) ->
    {bp_tree:tree_node(), bp_tree:key(), bp_tree:tree_node()}.
rotate_right(LNode = #bp_tree_node{leaf = true, children = LChildren},
    _ParentKey, RNode = #bp_tree_node{leaf = true, children = RChildren}) ->
    {ok, Key} = bp_tree_children:get({key, last}, LChildren),
    {ok, Value} = bp_tree_children:get({left, last}, LChildren),
    {ok, LChildren2, _} = bp_tree_children:remove({left, Key}, LChildren),
    {ok, RChildren2} = bp_tree_children:prepend(Key, Value, RChildren),
    {ok, ParentKey2} = bp_tree_children:get({key, last}, LChildren2),
    {
        LNode#bp_tree_node{children = LChildren2},
        ParentKey2,
        RNode#bp_tree_node{children = RChildren2}
    };
rotate_right(LNode = #bp_tree_node{leaf = false, children = LChildren},
    ParentKey, RNode = #bp_tree_node{leaf = false, children = RChildren}) ->
    {ok, Key} = bp_tree_children:get({key, last}, LChildren),
    {ok, Value} = bp_tree_children:get({right, last}, LChildren),
    {ok, LChildren2, _} = bp_tree_children:remove({right, Key}, LChildren),
    {ok, RChildren2} = bp_tree_children:prepend(ParentKey, Value, RChildren),
    {
        LNode#bp_tree_node{children = LChildren2},
        Key,
        RNode#bp_tree_node{children = RChildren2}
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Moves minimum value from a right sibling to a node.
%% @end
%%--------------------------------------------------------------------
-spec rotate_left(bp_tree:tree_node(), bp_tree:key(), bp_tree:tree_node()) ->
    {bp_tree:tree_node(), bp_tree:key(), bp_tree:tree_node()}.
rotate_left(LNode = #bp_tree_node{leaf = true, children = LChildren},
    _ParentKey, RNode = #bp_tree_node{leaf = true, children = RChildren}) ->
    {ok, Key} = bp_tree_children:get({key, first}, RChildren),
    {ok, Value} = bp_tree_children:get({left, first}, RChildren),
    {ok, RChildren2, _} = bp_tree_children:remove({left, Key}, RChildren),
    {ok, Next} = bp_tree_children:get({right, last}, LChildren),
    {ok, LChildren2} = bp_tree_children:append({both, Key}, {Value, Next}, LChildren),
    {
        LNode#bp_tree_node{children = LChildren2},
        Key,
        RNode#bp_tree_node{children = RChildren2}
    };
rotate_left(LNode = #bp_tree_node{leaf = false, children = LChildren},
    ParentKey, RNode = #bp_tree_node{leaf = false, children = RChildren}) ->
    {ok, Key} = bp_tree_children:get({key, first}, RChildren),
    {ok, Value} = bp_tree_children:get({left, first}, RChildren),
    {ok, RChildren2, _} = bp_tree_children:remove({left, Key}, RChildren),
    {ok, LChildren2} = bp_tree_children:append({right, ParentKey}, Value, LChildren),
    {
        LNode#bp_tree_node{children = LChildren2},
        Key,
        RNode#bp_tree_node{children = RChildren2}
    }.