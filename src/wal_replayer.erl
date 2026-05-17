%%% @doc Replays uncommitted WAL entries to the filesystem.
%%%
%%% Called on startup (or manually) to restore files that may have been
%%% lost to git reset, NFS failures, or pod restarts.  Only entries
%%% above the watermark are replayed — everything at or below is assumed
%%% committed to git.
-module(wal_replayer).

-include("wal.hrl").

-export([replay/1, replay/2]).

-type server_ref() :: atom() | pid() | {atom(), node()}
                    | {global, term()} | {via, atom(), term()}.

%% @doc Replay all uncommitted WAL entries from the legacy global writer
%%      to DataDir. Returns {ok, Count}.
-spec replay(binary()) -> {ok, non_neg_integer()} | {error, term()}.
replay(DataDir) ->
    replay(wal_writer, DataDir).

%% @doc Replay uncommitted entries from a specific writer instance to
%%      DataDir. Used when multiple writers run side by side
%%      (AUR-1354-h.2: one writer per user).
-spec replay(server_ref(), binary()) -> {ok, non_neg_integer()} | {error, term()}.
replay(ServerRef, DataDir) ->
    Entries = wal_writer:pending(ServerRef),
    Latest = latest_per_path(Entries),
    Count = maps:fold(fun(_Path, Entry, Acc) ->
        case apply_entry(DataDir, Entry) of
            ok -> Acc + 1;
            {error, Reason} ->
                logger:warning("wal_replayer: failed to replay ~s: ~p",
                               [Entry#wal_entry.path, Reason]),
                Acc
        end
    end, 0, Latest),
    logger:info("wal_replayer: replayed ~B entries to ~s", [Count, DataDir]),
    {ok, Count}.

%%% ===================================================================
%%% Internal
%%% ===================================================================

%% Keep only the latest entry for each path.
latest_per_path(Entries) ->
    lists:foldl(fun(#wal_entry{path = P, id = Id} = E, Acc) ->
        case maps:find(P, Acc) of
            {ok, #wal_entry{id = OldId}} when OldId >= Id ->
                Acc;
            _ ->
                maps:put(P, E, Acc)
        end
    end, #{}, Entries).

apply_entry(DataDir, #wal_entry{op = write, path = Path, content = Content}) ->
    FullPath = filename:join(DataDir, Path),
    case filelib:ensure_dir(FullPath) of
        ok -> file:write_file(FullPath, Content);
        {error, _} = Err -> Err
    end;

apply_entry(DataDir, #wal_entry{op = append, path = Path, content = Content}) ->
    FullPath = filename:join(DataDir, Path),
    case filelib:ensure_dir(FullPath) of
        ok -> file:write_file(FullPath, Content, [append]);
        {error, _} = Err -> Err
    end;

apply_entry(DataDir, #wal_entry{op = delete, path = Path}) ->
    FullPath = filename:join(DataDir, Path),
    case file:delete(FullPath) of
        ok -> ok;
        {error, enoent} -> ok;  %% already gone
        Err -> Err
    end.
