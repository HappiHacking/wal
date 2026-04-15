-module(wal_replayer_test).
-include_lib("eunit/include/eunit.hrl").
-include("wal.hrl").

setup() ->
    WalDir = tmp_dir("wal"),
    DataDir = tmp_dir("data"),
    ok = filelib:ensure_dir(filename:join(WalDir, "dummy")),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    catch gen_server:stop(wal_writer),
    timer:sleep(50),
    {ok, _} = wal_writer:start_link(WalDir),
    {WalDir, DataDir}.

cleanup({WalDir, DataDir}) ->
    catch gen_server:stop(wal_writer),
    os:cmd("rm -rf " ++ binary_to_list(WalDir)),
    os:cmd("rm -rf " ++ binary_to_list(DataDir)),
    ok.

tmp_dir(Prefix) ->
    N = integer_to_list(erlang:unique_integer([positive])),
    list_to_binary("/tmp/" ++ Prefix ++ "_test_" ++ N).

%% --- Tests ---

replay_restores_files_test() ->
    Dirs = {_WalDir, DataDir} = setup(),
    try
        ok = wal:write(<<"tasks/backlog.md">>, <<"## Today\n- [ ] Task 1\n">>, <<"test">>),
        ok = wal:write(<<"notes/today.md">>, <<"# Notes\n">>, <<"test">>),
        %% Files don't exist on disk yet (WAL only, no write-through in this test)
        false = filelib:is_file(filename:join(DataDir, <<"tasks/backlog.md">>)),
        %% Replay
        {ok, 2} = wal:replay(DataDir),
        %% Now they exist
        {ok, <<"## Today\n- [ ] Task 1\n">>} =
            file:read_file(filename:join(DataDir, <<"tasks/backlog.md">>)),
        {ok, <<"# Notes\n">>} =
            file:read_file(filename:join(DataDir, <<"notes/today.md">>))
    after
        cleanup(Dirs)
    end.

replay_only_pending_test() ->
    Dirs = {_WalDir, DataDir} = setup(),
    try
        ok = wal:write(<<"committed.md">>, <<"old">>, <<"test">>),
        ok = wal:advance_watermark(1),
        ok = wal:write(<<"pending.md">>, <<"new">>, <<"test">>),
        {ok, 1} = wal:replay(DataDir),
        %% Only the pending file is replayed
        false = filelib:is_file(filename:join(DataDir, <<"committed.md">>)),
        {ok, <<"new">>} = file:read_file(filename:join(DataDir, <<"pending.md">>))
    after
        cleanup(Dirs)
    end.

replay_uses_latest_version_test() ->
    Dirs = {_WalDir, DataDir} = setup(),
    try
        ok = wal:write(<<"a.md">>, <<"v1">>, <<"test">>),
        ok = wal:write(<<"a.md">>, <<"v2">>, <<"test">>),
        ok = wal:write(<<"a.md">>, <<"v3">>, <<"test">>),
        {ok, 1} = wal:replay(DataDir),
        {ok, <<"v3">>} = file:read_file(filename:join(DataDir, <<"a.md">>))
    after
        cleanup(Dirs)
    end.

replay_handles_delete_test() ->
    Dirs = {_WalDir, DataDir} = setup(),
    try
        ok = wal:write(<<"a.md">>, <<"content">>, <<"test">>),
        ok = wal:delete(<<"a.md">>, <<"test">>),
        {ok, 1} = wal:replay(DataDir),
        %% Delete entry means the file should not exist
        false = filelib:is_file(filename:join(DataDir, <<"a.md">>))
    after
        cleanup(Dirs)
    end.
