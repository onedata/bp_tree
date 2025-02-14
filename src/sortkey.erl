%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module providing abstraction for bp_tree link keys, which are
%%% used for sorting the entries.
%%% fixme napisac jak sie sortuja miedzy soba,
%%% opisac typy
%%% fixme opisac jak sie ma sortkey do indexu
%%% @end
%%%-------------------------------------------------------------------
-module(sortkey).

-include("bp_tree.hrl").


-export([pack_lexicographic/1, unpack_lexicographic/1]).
-export([pack_numeric/1, unpack_numeric/1]).
-export([pack_compound/1, unpack_compound/1]).
-export([to_index/1, from_index/1]).


-opaque lexicographic() :: binary().
-opaque numeric() :: integer().
-opaque compound() :: [binary() | integer()].
-export_type([lexicographic/0, numeric/0, compound/0]).

-type index() :: binary() | integer() | [binary() | integer()].
-export_type([index/0]).

%%%===================================================================
%%% API
%%%===================================================================


-spec pack_lexicographic(binary()) -> lexicographic().
pack_lexicographic(Binary) when is_binary(Binary) ->
    Binary.


-spec unpack_lexicographic(lexicographic()) -> binary().
unpack_lexicographic(Binary) when is_binary(Binary) ->
    Binary.


-spec pack_numeric(integer()) -> numeric().
pack_numeric(Int) when is_integer(Int) ->
    Int.


-spec unpack_numeric(numeric()) -> integer().
unpack_numeric(Int) when is_integer(Int) ->
    Int.


-spec pack_compound([binary() | integer() | compound()]) -> compound().
pack_compound(List) when is_list(List) ->
    lists:flatten(List).


-spec unpack_compound(compound()) -> [binary() | integer()].
unpack_compound(List) when is_list(List) ->
    List.


-spec to_index
    (lexicographic()) -> binary();
    (numeric()) -> integer();
    (compound()) -> [binary() | integer()].
to_index(Binary) when is_binary(Binary) ->
    unpack_lexicographic(Binary);
to_index(Integer) when is_integer(Integer) ->
    unpack_numeric(Integer);
to_index(List) when is_list(List) ->
    unpack_compound(List).


-spec from_index
    (binary()) -> lexicographic();
    (integer()) -> numeric();
    ([binary() | integer()]) -> compound().
from_index(Binary) when is_binary(Binary) ->
    pack_lexicographic(Binary);
from_index(Integer) when is_integer(Integer) ->
    pack_numeric(Integer);
from_index(List) when is_list(List) ->
    pack_compound(List).


%%%===================================================================
%%% Helpers  % fixme
%%%===================================================================
