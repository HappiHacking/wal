%%% @doc WAL entry encoding/decoding.
%%%
%%% Entries are stored as NDJSON — one JSON object per line.
%%% We use only stdlib (no jiffy/jsx dependency) by hand-encoding
%%% the small fixed-schema objects.
-module(wal_entry).

-include("wal.hrl").

-export([to_json/1, from_json/1, new/6]).

-spec new(atom(), binary(), binary(), binary(), map(), non_neg_integer()) ->
    #wal_entry{}.
new(Op, Path, Content, Origin, Meta, Id) ->
    #wal_entry{
        id      = Id,
        ts      = iso8601_now(),
        op      = Op,
        path    = Path,
        content = Content,
        origin  = Origin,
        meta    = Meta
    }.

%% @doc Encode entry to a single JSON line (no trailing newline).
-spec to_json(#wal_entry{}) -> binary().
to_json(#wal_entry{} = E) ->
    Parts = [
        {<<"id">>,      integer_to_binary(E#wal_entry.id)},
        {<<"ts">>,      E#wal_entry.ts},
        {<<"op">>,      atom_to_binary(E#wal_entry.op, utf8)},
        {<<"path">>,    E#wal_entry.path},
        {<<"content">>, E#wal_entry.content},
        {<<"origin">>,  E#wal_entry.origin},
        {<<"meta">>,    encode_meta(E#wal_entry.meta)}
    ],
    iolist_to_binary([
        <<"{">>,
        lists:join(<<",">>, [json_kv(K, V) || {K, V} <- Parts]),
        <<"}">>
    ]).

%% @doc Decode a single JSON line to a wal_entry record.
-spec from_json(binary()) -> {ok, #wal_entry{}} | {error, term()}.
from_json(Line) ->
    try
        Map = json_decode(Line),
        {ok, #wal_entry{
            id      = binary_to_integer(maps:get(<<"id">>, Map)),
            ts      = maps:get(<<"ts">>, Map),
            op      = binary_to_existing_atom(maps:get(<<"op">>, Map), utf8),
            path    = maps:get(<<"path">>, Map),
            content = maps:get(<<"content">>, Map, <<>>),
            origin  = maps:get(<<"origin">>, Map),
            meta    = decode_meta(maps:get(<<"meta">>, Map, <<"{}">>))
        }}
    catch
        _:Reason -> {error, Reason}
    end.

%%% Internal JSON helpers — minimal, no dependencies.
%%% We only need to handle our fixed schema, not arbitrary JSON.

json_kv(Key, Value) when is_binary(Key), is_binary(Value) ->
    [<<"\"">>, Key, <<"\":\"">>, json_escape(Value), <<"\"">>];
json_kv(Key, Value) when is_binary(Key) ->
    %% For pre-encoded values (like meta object or integers)
    [<<"\"">>, Key, <<":">>, Value].

json_escape(Bin) ->
    %% Escape \, ", newlines, tabs for JSON string safety
    binary:replace(
        binary:replace(
            binary:replace(
                binary:replace(
                    binary:replace(Bin, <<"\\">>, <<"\\\\">>, [global]),
                    <<"\"">>, <<"\\\"">>, [global]),
                <<"\n">>, <<"\\n">>, [global]),
            <<"\r">>, <<"\\r">>, [global]),
        <<"\t">>, <<"\\t">>, [global]).

encode_meta(Meta) when is_map(Meta), map_size(Meta) == 0 ->
    <<"{}">>;
encode_meta(Meta) when is_map(Meta) ->
    Pairs = maps:fold(fun(K, V, Acc) ->
        Key = if is_atom(K) -> atom_to_binary(K, utf8);
                 is_binary(K) -> K
              end,
        Val = if is_binary(V) -> V;
                 is_atom(V) -> atom_to_binary(V, utf8);
                 is_integer(V) -> integer_to_binary(V)
              end,
        [[<<"\"">>, json_escape(Key), <<"\":\"">>, json_escape(Val), <<"\"">>] | Acc]
    end, [], Meta),
    iolist_to_binary([<<"{">>, lists:join(<<",">>, Pairs), <<"}">>]).

%% OTP 27 has json module — use it for decoding
json_decode(Bin) ->
    {Map, _, _} = json:decode(Bin, ok, #{}),
    Map.

decode_meta(Bin) when is_binary(Bin) ->
    case json_decode(Bin) of
        M when is_map(M) -> M;
        _ -> #{}
    end;
decode_meta(M) when is_map(M) -> M;
decode_meta(_) -> #{}.

iso8601_now() ->
    {{Y, Mo, D}, {H, Mi, S}} = calendar:universal_time(),
    iolist_to_binary(io_lib:format("~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0BZ",
                                    [Y, Mo, D, H, Mi, S])).
