%%% @doc Public API for the Write-Ahead Log.
%%%
%%% Every write goes through the WAL before (or simultaneously with) the
%%% target file on disk.  The WAL is an append-only NDJSON file on local
%%% storage.  A sync watermark tracks which entries have been committed
%%% and pushed to git.  On startup, uncommitted entries are replayed to
%%% restore any files lost to git reset or NFS failures.
-module(wal).

-include("wal.hrl").

-export([write/3, write/4,
         append/3, append/4,
         delete/2, delete/3,
         read/1,
         pending/0,
         pending_count/0,
         advance_watermark/1,
         replay/1]).

%% @doc Write (overwrite) a file.  The content replaces whatever was there.
-spec write(binary(), binary(), binary()) -> ok | {error, term()}.
write(Path, Content, Origin) ->
    write(Path, Content, Origin, #{}).

-spec write(binary(), binary(), binary(), map()) -> ok | {error, term()}.
write(Path, Content, Origin, Meta) ->
    wal_writer:append_entry(write, Path, Content, Origin, Meta).

%% @doc Append content to a file (e.g. CSV row).
-spec append(binary(), binary(), binary()) -> ok | {error, term()}.
append(Path, Content, Origin) ->
    append(Path, Content, Origin, #{}).

-spec append(binary(), binary(), binary(), map()) -> ok | {error, term()}.
append(Path, Content, Origin, Meta) ->
    wal_writer:append_entry(append, Path, Content, Origin, Meta).

%% @doc Delete a file.
-spec delete(binary(), binary()) -> ok | {error, term()}.
delete(Path, Origin) ->
    delete(Path, Origin, #{}).

-spec delete(binary(), binary(), map()) -> ok | {error, term()}.
delete(Path, Origin, Meta) ->
    wal_writer:append_entry(delete, Path, <<>>, Origin, Meta).

%% @doc Read the current content for a path from the WAL.
%%      Returns the content from the most recent write/append entry,
%%      or {error, not_found} if no entry exists for the path.
-spec read(binary()) -> {ok, binary()} | {error, not_found}.
read(Path) ->
    wal_writer:read_path(Path).

%% @doc Return all entries with ID > watermark (uncommitted).
-spec pending() -> [#wal_entry{}].
pending() ->
    wal_writer:pending().

%% @doc Count of uncommitted entries.
-spec pending_count() -> non_neg_integer().
pending_count() ->
    length(pending()).

%% @doc Advance the watermark after a successful git push.
%%      Entries at or below this ID are considered committed.
%%      Pruning happens lazily.
-spec advance_watermark(non_neg_integer()) -> ok.
advance_watermark(Id) ->
    wal_writer:advance_watermark(Id).

%% @doc Replay uncommitted entries to the given data directory.
%%      Called on startup or after a git reset.
-spec replay(binary()) -> {ok, non_neg_integer()} | {error, term()}.
replay(DataDir) ->
    wal_replayer:replay(DataDir).
