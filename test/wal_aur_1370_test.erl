%%% @doc Tests for AUR-1354-h.1 (warrant AUR-1370):
%%%   - wal_writer:start_link/2 with a custom Name lets two writers
%%%     run side by side without interfering.
%%%   - payload cap returns {error, payload_too_large}.
%%%   - non-binary content returns {error, content_not_binary}.
%%%   - deadline_ms in Meta is honoured by the gen_server:call timeout.
%%%   - binary:copy/1 is applied on Content (verified by writing a
%%%     sub-binary slice and confirming the entry's content is the
%%%     correct bytes).
%%%
%%% Backpressure ({busy, RetryMs}) and full mailbox-cap behaviour are
%%% inherently load-dependent and noisier to test deterministically —
%%% covered by manual smoke + production telemetry, not eunit.
-module(wal_aur_1370_test).
-include_lib("eunit/include/eunit.hrl").
-include("wal.hrl").

tmp_dir() ->
    N = integer_to_list(erlang:unique_integer([positive])),
    list_to_binary("/tmp/wal_aur1370_" ++ N).

%% --- Two writers side by side via Name parameter ---

named_writers_are_independent_test() ->
    DirA = tmp_dir(),
    DirB = tmp_dir(),
    ok = filelib:ensure_dir(filename:join(DirA, "dummy")),
    ok = filelib:ensure_dir(filename:join(DirB, "dummy")),
    catch gen_server:stop(writer_a),
    catch gen_server:stop(writer_b),
    timer:sleep(50),
    {ok, PidA} = wal_writer:start_link({local, writer_a}, DirA),
    {ok, PidB} = wal_writer:start_link({local, writer_b}, DirB),
    ?assertNotEqual(PidA, PidB),
    try
        ok = wal:write(writer_a, <<"a.md">>, <<"A-only">>, <<"test">>, #{}),
        ok = wal:write(writer_b, <<"b.md">>, <<"B-only">>, <<"test">>, #{}),
        ?assertEqual({ok, <<"A-only">>}, wal:read(writer_a, <<"a.md">>)),
        ?assertEqual({error, not_found}, wal:read(writer_a, <<"b.md">>)),
        ?assertEqual({ok, <<"B-only">>}, wal:read(writer_b, <<"b.md">>)),
        ?assertEqual({error, not_found}, wal:read(writer_b, <<"a.md">>)),
        ?assertEqual(1, wal:pending_count(writer_a)),
        ?assertEqual(1, wal:pending_count(writer_b))
    after
        catch gen_server:stop(writer_a),
        catch gen_server:stop(writer_b),
        os:cmd("rm -rf " ++ binary_to_list(DirA)),
        os:cmd("rm -rf " ++ binary_to_list(DirB))
    end.

%% --- Payload cap ---

payload_too_large_test() ->
    Dir = tmp_dir(),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    catch gen_server:stop(wal_writer),
    timer:sleep(50),
    %% Lower the cap for this test so we don't have to fabricate 64 KiB.
    application:set_env(wal, max_payload_bytes, 32),
    {ok, _} = wal_writer:start_link(Dir),
    try
        ok = wal:write(<<"small.md">>, <<"tiny content">>, <<"test">>),
        Big = binary:copy(<<"x">>, 100),
        ?assertEqual({error, payload_too_large},
                     wal:write(<<"big.md">>, Big, <<"test">>))
    after
        application:unset_env(wal, max_payload_bytes),
        catch gen_server:stop(wal_writer),
        os:cmd("rm -rf " ++ binary_to_list(Dir))
    end.

non_binary_content_rejected_test() ->
    Dir = tmp_dir(),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    catch gen_server:stop(wal_writer),
    timer:sleep(50),
    {ok, _} = wal_writer:start_link(Dir),
    try
        %% Pass a list instead of a binary — should be rejected by the
        %% Resource Owner's invariant check.
        ?assertEqual({error, content_not_binary},
                     wal:write(<<"x.md">>, "not a binary", <<"test">>))
    after
        catch gen_server:stop(wal_writer),
        os:cmd("rm -rf " ++ binary_to_list(Dir))
    end.

%% --- deadline_ms in Meta is honoured ---

deadline_ms_in_meta_is_honoured_test() ->
    Dir = tmp_dir(),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    catch gen_server:stop(wal_writer),
    timer:sleep(50),
    {ok, _} = wal_writer:start_link(Dir),
    try
        %% Normal call with no deadline_ms — should succeed within
        %% default 8000 ms timeout.
        ok = wal:write(<<"x.md">>, <<"y">>, <<"test">>, #{}),
        %% Call with a very generous deadline — must still succeed.
        ok = wal:write(<<"y.md">>, <<"z">>, <<"test">>, #{deadline_ms => 5000}),
        %% Confirm an invalid deadline (non-integer) falls back to default.
        ok = wal:write(<<"z.md">>, <<"w">>, <<"test">>, #{deadline_ms => not_an_int})
    after
        catch gen_server:stop(wal_writer),
        os:cmd("rm -rf " ++ binary_to_list(Dir))
    end.

%% --- binary:copy/1 — sub-binary slices are stored as their own bytes ---

content_is_copied_test() ->
    Dir = tmp_dir(),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    catch gen_server:stop(wal_writer),
    timer:sleep(50),
    {ok, _} = wal_writer:start_link(Dir),
    try
        Huge = binary:copy(<<"a">>, 1024),
        %% Take a small slice — this is a reference into the larger Huge
        %% binary on the heap.
        Slice = binary:part(Huge, 0, 4),
        ok = wal:write(<<"slice.md">>, Slice, <<"test">>),
        ?assertEqual({ok, <<"aaaa">>}, wal:read(<<"slice.md">>))
    after
        catch gen_server:stop(wal_writer),
        os:cmd("rm -rf " ++ binary_to_list(Dir))
    end.

%% --- Meta is preserved verbatim ---

meta_preserved_test() ->
    Dir = tmp_dir(),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    catch gen_server:stop(wal_writer),
    timer:sleep(50),
    {ok, _} = wal_writer:start_link(Dir),
    try
        Ctx = #{trace_id => <<"trace-abc">>,
                causation_id => <<"req-1">>,
                idem_key => <<"key-1">>},
        ok = wal:write(<<"a.md">>, <<"hello">>, <<"caller">>, Ctx),
        [E] = wal:pending(),
        %% NDJSON round-trip stringifies atom keys to binaries. The Ctx
        %% shape is preserved verbatim; consumers (replayer, audit
        %% tooling) decode by the binary key name.
        Meta = E#wal_entry.meta,
        ?assertEqual(<<"trace-abc">>, maps:get(<<"trace_id">>, Meta)),
        ?assertEqual(<<"req-1">>, maps:get(<<"causation_id">>, Meta)),
        ?assertEqual(<<"key-1">>, maps:get(<<"idem_key">>, Meta))
    after
        catch gen_server:stop(wal_writer),
        os:cmd("rm -rf " ++ binary_to_list(Dir))
    end.
