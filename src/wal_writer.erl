%%% @doc Serialized WAL writer — gen_server that owns the log file handle.
%%%
%%% All WAL appends go through this process to guarantee ordering and
%%% crash-safe writes (fsync after every entry).  The watermark file
%%% tracks the last entry confirmed pushed to git.
-module(wal_writer).
-behaviour(gen_server).

-include("wal.hrl").

%% API
-export([start_link/1,
         append_entry/5,
         read_path/1,
         pending/0,
         advance_watermark/1,
         current_watermark/0,
         max_id/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    wal_dir    :: binary(),
    log_path   :: binary(),
    wm_path    :: binary(),
    fd         :: file:io_device() | undefined,
    next_id    :: non_neg_integer(),
    watermark  :: non_neg_integer()
}).

%%% ===================================================================
%%% API
%%% ===================================================================

start_link(WalDir) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, WalDir, []).

-spec append_entry(atom(), binary(), binary(), binary(), map()) ->
    ok | {error, term()}.
append_entry(Op, Path, Content, Origin, Meta) ->
    gen_server:call(?MODULE, {append, Op, Path, Content, Origin, Meta}, 10_000).

-spec read_path(binary()) -> {ok, binary()} | {error, not_found}.
read_path(Path) ->
    gen_server:call(?MODULE, {read_path, Path}, 5_000).

-spec pending() -> [#wal_entry{}].
pending() ->
    gen_server:call(?MODULE, pending, 10_000).

-spec advance_watermark(non_neg_integer()) -> ok.
advance_watermark(Id) ->
    gen_server:call(?MODULE, {advance_watermark, Id}, 5_000).

-spec current_watermark() -> non_neg_integer().
current_watermark() ->
    gen_server:call(?MODULE, current_watermark, 5_000).

-spec max_id() -> non_neg_integer().
max_id() ->
    gen_server:call(?MODULE, max_id, 5_000).

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
        wal_dir   = DirBin,
        log_path  = LogPath,
        wm_path   = WmPath,
        fd        = Fd,
        next_id   = NextId,
        watermark = Watermark
    }}.

handle_call({append, Op, Path, Content, Origin, Meta}, _From, State) ->
    Id = State#state.next_id,
    Entry = wal_entry:new(Op, Path, Content, Origin, Meta, Id),
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
    end;

handle_call({read_path, Path}, _From, State) ->
    %% Scan log for most recent write/append to Path
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
%%% Internal
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
            %% Walk backwards to find most recent entry for path
            scan_lines_reverse(lists:reverse(Lines), TargetPath);
        {error, _} ->
            {error, not_found}
    end.

scan_lines_reverse([], _Path) ->
    {error, not_found};
scan_lines_reverse([Line | Rest], Path) ->
    case wal_entry:from_json(Line) of
        {ok, #wal_entry{path = P, op = delete}} when P == Path ->
            {error, not_found};  %% deleted
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

%% Prune committed entries — rewrite log keeping only entries above watermark.
%% Only prune when the file has grown (>100 committed entries to avoid churn).
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
