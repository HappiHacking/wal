%%% @doc Serialized WAL writer — gen_server that owns the log file handle.
%%%
%%% Role:       Resource Owner
%%% State:      NDJSON log handle, watermark, next entry id (per-instance)
%%% Protocol:   wal:write/5, wal:append/5, wal:delete/4, wal:read/2,
%%%             wal:pending/1, wal:advance_watermark/2 (or the legacy
%%%             Name-less variants that target the registered `wal_writer`)
%%% Failure:    Restart re-opens the NDJSON file at end-of-file; the
%%%             watermark file is read on init; uncommitted entries can be
%%%             replayed via wal:replay/1 once the data directory is known
%%% Supervisor: wal_sup (legacy global instance) or external Erlang
%%%             supervisor for per-user instances (see aurora_wal_sup,
%%%             AUR-1354-h.2)
%%%
%%% All WAL appends go through this process to guarantee ordering and
%%% crash-safe writes (fsync after every entry). The watermark file
%%% tracks the last entry confirmed pushed to git.
%%%
%%% AUR-1354-h.1: API extended to take an optional Name first arg so
%%% multiple instances can run side by side (one per user). Backwards
%%% compatible — the no-Name variants target ?MODULE as before.
%%%
%%% Constraints (per BEAM_RULES.md in the consuming aurora-beam-server):
%%% - S12 exemption: NDJSON appends are small (<= max_payload_bytes,
%%%   default 64 KiB) and treated as bounded local I/O. Payloads above
%%%   the cap are rejected with {error, payload_too_large}.
%%% - S3 backpressure: when message_queue_len exceeds the configured cap
%%%   (default 1000) the writer replies {busy, RetryMs} immediately
%%%   without journaling, so callers can shed load.
%%% - Part VI binary hygiene: incoming Content is copied via binary:copy/1
%%%   before being held in the entry record, so off-heap binary
%%%   references don't accumulate via mailbox retention.
-module(wal_writer).
-behaviour(gen_server).

-include("wal.hrl").

%% API — Name-parameterised (new in AUR-1354-h.1); single-arg variants
%% remain for backwards compatibility with the legacy global instance.
-export([start_link/1, start_link/2,
         append_entry/5, append_entry/6,
         read_path/1, read_path/2,
         pending/0, pending/1,
         advance_watermark/1, advance_watermark/2,
         current_watermark/0, current_watermark/1,
         max_id/0, max_id/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DEFAULT_MAX_PAYLOAD_BYTES, 65536).
-define(DEFAULT_MAILBOX_CAP, 1000).
-define(DEFAULT_RETRY_MS, 250).
-define(DEFAULT_CALL_TIMEOUT_MS, 8000).

-type server_ref() :: atom() | pid() | {atom(), node()}
                    | {global, term()} | {via, atom(), term()}.

-record(state, {
    wal_dir        :: binary(),
    log_path       :: binary(),
    wm_path        :: binary(),
    fd             :: file:io_device() | undefined,
    next_id        :: non_neg_integer(),
    watermark      :: non_neg_integer(),
    max_payload    :: pos_integer(),
    mailbox_cap    :: pos_integer(),
    retry_ms       :: pos_integer()
}).

%%% ===================================================================
%%% API
%%% ===================================================================

-spec start_link(binary() | string()) -> {ok, pid()} | {error, term()}.
start_link(WalDir) ->
    start_link({local, ?MODULE}, WalDir).

-spec start_link(Name, binary() | string()) -> {ok, pid()} | {error, term()}
        when Name :: {local, atom()} | {global, term()} | {via, atom(), term()}.
start_link(Name, WalDir) ->
    gen_server:start_link(Name, ?MODULE, WalDir, []).

-spec append_entry(atom(), binary(), binary(), binary(), map()) ->
    ok | {busy, pos_integer()} | {error, term()}.
append_entry(Op, Path, Content, Origin, Meta) ->
    append_entry(?MODULE, Op, Path, Content, Origin, Meta).

-spec append_entry(server_ref(), atom(), binary(), binary(), binary(), map()) ->
    ok | {busy, pos_integer()} | {error, term()}.
append_entry(ServerRef, Op, Path, Content, Origin, Meta) ->
    Timeout = call_timeout(Meta),
    gen_server:call(ServerRef, {append, Op, Path, Content, Origin, Meta}, Timeout).

-spec read_path(binary()) -> {ok, binary()} | {error, not_found}.
read_path(Path) ->
    read_path(?MODULE, Path).

-spec read_path(server_ref(), binary()) -> {ok, binary()} | {error, not_found}.
read_path(ServerRef, Path) ->
    gen_server:call(ServerRef, {read_path, Path}, ?DEFAULT_CALL_TIMEOUT_MS).

-spec pending() -> [#wal_entry{}].
pending() ->
    pending(?MODULE).

-spec pending(server_ref()) -> [#wal_entry{}].
pending(ServerRef) ->
    gen_server:call(ServerRef, pending, ?DEFAULT_CALL_TIMEOUT_MS).

-spec advance_watermark(non_neg_integer()) -> ok.
advance_watermark(Id) ->
    advance_watermark(?MODULE, Id).

-spec advance_watermark(server_ref(), non_neg_integer()) -> ok.
advance_watermark(ServerRef, Id) ->
    gen_server:call(ServerRef, {advance_watermark, Id}, ?DEFAULT_CALL_TIMEOUT_MS).

-spec current_watermark() -> non_neg_integer().
current_watermark() ->
    current_watermark(?MODULE).

-spec current_watermark(server_ref()) -> non_neg_integer().
current_watermark(ServerRef) ->
    gen_server:call(ServerRef, current_watermark, ?DEFAULT_CALL_TIMEOUT_MS).

-spec max_id() -> non_neg_integer().
max_id() ->
    max_id(?MODULE).

-spec max_id(server_ref()) -> non_neg_integer().
max_id(ServerRef) ->
    gen_server:call(ServerRef, max_id, ?DEFAULT_CALL_TIMEOUT_MS).

%%% ===================================================================
%%% gen_server callbacks
%%% ===================================================================

init(WalDir) ->
    DirBin = to_bin(WalDir),
    ok = filelib:ensure_dir(filename:join(DirBin, "dummy")),
    LogPath = filename:join(DirBin, <<"events.ndjson">>),
    WmPath  = filename:join(DirBin, <<"watermark">>),
    Watermark = read_watermark(WmPath),
    {NextId, Fd} = open_log(LogPath),
    {ok, #state{
        wal_dir     = DirBin,
        log_path    = LogPath,
        wm_path     = WmPath,
        fd          = Fd,
        next_id     = NextId,
        watermark   = Watermark,
        max_payload = application:get_env(wal, max_payload_bytes, ?DEFAULT_MAX_PAYLOAD_BYTES),
        mailbox_cap = application:get_env(wal, mailbox_cap, ?DEFAULT_MAILBOX_CAP),
        retry_ms    = application:get_env(wal, retry_ms, ?DEFAULT_RETRY_MS)
    }}.

handle_call({append, Op, Path, Content, Origin, Meta}, _From, State) ->
    case overloaded(State) of
        true ->
            {reply, {busy, State#state.retry_ms}, State};
        false ->
            case validate_payload(Content, State) of
                ok ->
                    do_append(Op, Path, Content, Origin, Meta, State);
                {error, _} = Err ->
                    {reply, Err, State}
            end
    end;

handle_call({read_path, Path}, _From, State) ->
    Result = scan_for_path(State#state.log_path, Path),
    {reply, Result, State};

handle_call(pending, _From, State) ->
    Entries = read_entries_above(State#state.log_path, State#state.watermark),
    {reply, Entries, State};

handle_call({advance_watermark, Id}, _From, State) ->
    ok = write_watermark(State#state.wm_path, Id),
    maybe_prune(State#state.log_path, Id),
    {reply, ok, State#state{watermark = Id}};

handle_call(current_watermark, _From, State) ->
    {reply, State#state.watermark, State};

handle_call(max_id, _From, State) ->
    {reply, State#state.next_id - 1, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{fd = Fd}) when Fd =/= undefined ->
    file:close(Fd),
    ok;
terminate(_Reason, _State) ->
    ok.

%%% ===================================================================
%%% Internal — admission control
%%% ===================================================================

overloaded(#state{mailbox_cap = Cap}) ->
    case erlang:process_info(self(), message_queue_len) of
        {message_queue_len, Len} when Len > Cap ->
            logger:warning(#{event => wal_writer_busy, queue_len => Len, cap => Cap}),
            true;
        _ ->
            false
    end.

validate_payload(Content, #state{max_payload = MaxBytes}) when is_binary(Content) ->
    case byte_size(Content) of
        Sz when Sz > MaxBytes ->
            logger:warning(#{event => wal_writer_payload_too_large,
                             size => Sz, max => MaxBytes}),
            {error, payload_too_large};
        _ ->
            ok
    end;
validate_payload(_, _) ->
    {error, content_not_binary}.

call_timeout(#{deadline_ms := DeadlineMs}) when is_integer(DeadlineMs), DeadlineMs > 0 ->
    DeadlineMs;
call_timeout(_) ->
    ?DEFAULT_CALL_TIMEOUT_MS.

%%% ===================================================================
%%% Internal — append
%%% ===================================================================

do_append(Op, Path, Content, Origin, Meta, State) ->
    Id = State#state.next_id,
    %% Part VI binary hygiene: copy off the shared binary so this
    %% long-lived process doesn't retain a reference to whatever larger
    %% binary the caller may have sliced Content from.
    SafeContent = binary:copy(Content),
    Entry = wal_entry:new(Op, Path, SafeContent, Origin, Meta, Id),
    Line = <<(wal_entry:to_json(Entry))/binary, "\n">>,
    case file:write(State#state.fd, Line) of
        ok ->
            case file:sync(State#state.fd) of
                ok ->
                    {reply, ok, State#state{next_id = Id + 1}};
                {error, _} = Err ->
                    {reply, Err, State}
            end;
        {error, _} = Err ->
            {reply, Err, State}
    end.

%%% ===================================================================
%%% Internal — log management (unchanged from previous version)
%%% ===================================================================

open_log(LogPath) ->
    MaxId = case file:read_file(LogPath) of
        {ok, Data} ->
            Lines = binary:split(Data, <<"\n">>, [global, trim_all]),
            lists:foldl(fun(Line, Acc) ->
                case wal_entry:from_json(Line) of
                    {ok, E} -> max(E#wal_entry.id, Acc);
                    _ -> Acc
                end
            end, 0, Lines);
        {error, enoent} ->
            0
    end,
    {ok, Fd} = file:open(LogPath, [append, raw, binary]),
    {MaxId + 1, Fd}.

read_watermark(Path) ->
    case file:read_file(Path) of
        {ok, Data} ->
            case string:trim(binary_to_list(Data)) of
                "" -> 0;
                S ->
                    case string:to_integer(S) of
                        {Int, _} -> Int;
                        _ -> 0
                    end
            end;
        {error, enoent} ->
            0
    end.

write_watermark(Path, Id) ->
    Data = integer_to_binary(Id),
    TmpPath = <<Path/binary, ".tmp">>,
    ok = file:write_file(TmpPath, Data),
    ok = file:rename(TmpPath, Path).

scan_for_path(LogPath, TargetPath) ->
    case file:read_file(LogPath) of
        {ok, Data} ->
            Lines = binary:split(Data, <<"\n">>, [global, trim_all]),
            scan_lines_reverse(lists:reverse(Lines), TargetPath);
        {error, _} ->
            {error, not_found}
    end.

scan_lines_reverse([], _Path) ->
    {error, not_found};
scan_lines_reverse([Line | Rest], Path) ->
    case wal_entry:from_json(Line) of
        {ok, #wal_entry{path = P, op = delete}} when P == Path ->
            {error, not_found};
        {ok, #wal_entry{path = P, content = C}} when P == Path ->
            {ok, C};
        _ ->
            scan_lines_reverse(Rest, Path)
    end.

read_entries_above(LogPath, Watermark) ->
    case file:read_file(LogPath) of
        {ok, Data} ->
            Lines = binary:split(Data, <<"\n">>, [global, trim_all]),
            lists:filtermap(fun(Line) ->
                case wal_entry:from_json(Line) of
                    {ok, #wal_entry{id = Id} = E} when Id > Watermark ->
                        {true, E};
                    _ ->
                        false
                end
            end, Lines);
        {error, _} ->
            []
    end.

maybe_prune(LogPath, Watermark) ->
    case file:read_file(LogPath) of
        {ok, Data} ->
            Lines = binary:split(Data, <<"\n">>, [global, trim_all]),
            {Keep, Pruned} = lists:partition(fun(Line) ->
                case wal_entry:from_json(Line) of
                    {ok, #wal_entry{id = Id}} -> Id > Watermark;
                    _ -> false
                end
            end, Lines),
            case length(Pruned) > 100 of
                true ->
                    NewData = case Keep of
                        [] -> <<>>;
                        _ -> iolist_to_binary([lists:join(<<"\n">>, Keep), <<"\n">>])
                    end,
                    TmpPath = <<LogPath/binary, ".tmp">>,
                    ok = file:write_file(TmpPath, NewData),
                    ok = file:rename(TmpPath, LogPath);
                false ->
                    ok
            end;
        {error, _} ->
            ok
    end.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_list(V) -> list_to_binary(V).
