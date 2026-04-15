-module(wal_writer_test).
-include_lib("eunit/include/eunit.hrl").
-include("wal.hrl").

%% Each test gets a fresh temp dir and a fresh wal_writer process.

setup() ->
    TmpDir = tmp_dir(),
    ok = filelib:ensure_dir(filename:join(TmpDir, "dummy")),
    %% Stop any running writer
    catch gen_server:stop(wal_writer),
    timer:sleep(50),
    {ok, _Pid} = wal_writer:start_link(TmpDir),
    TmpDir.

cleanup(TmpDir) ->
    catch gen_server:stop(wal_writer),
    os:cmd("rm -rf " ++ binary_to_list(TmpDir)),
    ok.

tmp_dir() ->
    N = integer_to_list(erlang:unique_integer([positive])),
    Dir = list_to_binary("/tmp/wal_test_" ++ N),
    Dir.

%% --- Tests ---

append_and_read_test() ->
    Dir = setup(),
    try
        ok = wal:write(<<"tasks/backlog.md">>, <<"## Today\n">>, <<"test">>),
        {ok, <<"## Today\n">>} = wal:read(<<"tasks/backlog.md">>),
        {error, not_found} = wal:read(<<"nonexistent">>)
    after
        cleanup(Dir)
    end.

sequential_ids_test() ->
    Dir = setup(),
    try
        ok = wal:write(<<"a.md">>, <<"1">>, <<"test">>),
        ok = wal:write(<<"b.md">>, <<"2">>, <<"test">>),
        ok = wal:write(<<"c.md">>, <<"3">>, <<"test">>),
        3 = wal_writer:max_id()
    after
        cleanup(Dir)
    end.

pending_returns_uncommitted_test() ->
    Dir = setup(),
    try
        ok = wal:write(<<"a.md">>, <<"1">>, <<"test">>),
        ok = wal:write(<<"b.md">>, <<"2">>, <<"test">>),
        2 = wal:pending_count(),
        ok = wal:advance_watermark(1),
        1 = wal:pending_count(),
        [E] = wal:pending(),
        <<"b.md">> = E#wal_entry.path
    after
        cleanup(Dir)
    end.

watermark_persists_test() ->
    Dir = setup(),
    try
        ok = wal:write(<<"a.md">>, <<"1">>, <<"test">>),
        ok = wal:advance_watermark(1),
        %% Restart writer
        gen_server:stop(wal_writer),
        timer:sleep(50),
        {ok, _} = wal_writer:start_link(Dir),
        1 = wal_writer:current_watermark()
    after
        cleanup(Dir)
    end.

delete_shadows_write_test() ->
    Dir = setup(),
    try
        ok = wal:write(<<"a.md">>, <<"hello">>, <<"test">>),
        {ok, <<"hello">>} = wal:read(<<"a.md">>),
        ok = wal:delete(<<"a.md">>, <<"test">>),
        {error, not_found} = wal:read(<<"a.md">>)
    after
        cleanup(Dir)
    end.

log_survives_restart_test() ->
    Dir = setup(),
    try
        ok = wal:write(<<"a.md">>, <<"content1">>, <<"test">>),
        ok = wal:write(<<"b.md">>, <<"content2">>, <<"test">>),
        %% Restart
        gen_server:stop(wal_writer),
        timer:sleep(50),
        {ok, _} = wal_writer:start_link(Dir),
        %% IDs continue from where they left off
        2 = wal_writer:max_id(),
        %% Content still readable
        {ok, <<"content1">>} = wal:read(<<"a.md">>),
        {ok, <<"content2">>} = wal:read(<<"b.md">>)
    after
        cleanup(Dir)
    end.

overwrite_returns_latest_test() ->
    Dir = setup(),
    try
        ok = wal:write(<<"a.md">>, <<"v1">>, <<"test">>),
        ok = wal:write(<<"a.md">>, <<"v2">>, <<"test">>),
        ok = wal:write(<<"a.md">>, <<"v3">>, <<"test">>),
        {ok, <<"v3">>} = wal:read(<<"a.md">>)
    after
        cleanup(Dir)
    end.
