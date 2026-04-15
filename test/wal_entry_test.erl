-module(wal_entry_test).
-include_lib("eunit/include/eunit.hrl").
-include("wal.hrl").

roundtrip_test() ->
    E = wal_entry:new(write, <<"tasks/backlog.md">>, <<"## Today\n">>,
                      <<"BacklogEditor">>, #{message => <<"add task">>}, 42),
    Json = wal_entry:to_json(E),
    {ok, Decoded} = wal_entry:from_json(Json),
    42 = Decoded#wal_entry.id,
    write = Decoded#wal_entry.op,
    <<"tasks/backlog.md">> = Decoded#wal_entry.path,
    <<"## Today\n">> = Decoded#wal_entry.content,
    <<"BacklogEditor">> = Decoded#wal_entry.origin.

escaping_test() ->
    Content = <<"line1\nline2\n\"quoted\"\ttab\\back">>,
    E = wal_entry:new(write, <<"test.md">>, Content, <<"test">>, #{}, 1),
    Json = wal_entry:to_json(E),
    %% Should not contain raw newlines (would break NDJSON)
    nomatch = binary:match(Json, <<"\n">>),
    %% Roundtrip preserves content
    {ok, D} = wal_entry:from_json(Json),
    Content = D#wal_entry.content.

empty_meta_test() ->
    E = wal_entry:new(delete, <<"old.md">>, <<>>, <<"cleanup">>, #{}, 99),
    Json = wal_entry:to_json(E),
    {ok, D} = wal_entry:from_json(Json),
    99 = D#wal_entry.id,
    delete = D#wal_entry.op.
