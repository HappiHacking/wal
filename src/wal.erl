%%% @doc Public API for the Write-Ahead Log.
%%%
%%% Every write goes through the WAL before (or simultaneously with) the
%%% target file on disk.  The WAL is an append-only NDJSON file on local
%%% storage.  A sync watermark tracks which entries have been committed
%%% and pushed to git.  On startup, uncommitted entries are replayed to
%%% restore any files lost to git reset or NFS failures.
%%%
%%% Two flavours of API:
%%%
%%% * Legacy single-instance — targets the registered `wal_writer`
%%%   started by `wal_sup`. Used by existing aurora-beam-server code
%%%   that pre-dates the per-user split (AUR-1354-h).
%%%
%%% * Name-parameterised — takes a server reference as the first arg.
%%%   Used by AUR-1354-h.2+ when one `wal_writer` runs per user.
%%%
%%% ## Meta / Ctx
%%%
%%% The `Meta` map is opaque to the WAL itself but the WAL recognises
%%% one well-known key:
%%%
%%%   `deadline_ms` — overrides the default 8000 ms gen_server:call
%%%   timeout for this single call. Used by upstream code that
%%%   propagates a per-request budget.
%%%
%%% Aurora's higher-level write API (`aurora_user_scope`, AUR-1354-h.3)
%%% standardises the rest of the Meta keys as a `Ctx` map:
%%%
%%%   #{trace_id       := binary(),    %% required at cross-domain boundary
%%%     causation_id   := binary(),    %% required at cross-domain boundary
%%%     idem_key       => binary(),    %% required iff caller may retry
%%%     schema_version => non_neg_integer(),
%%%     sent_at        => integer()}.
%%%
%%% The WAL persists Meta verbatim alongside the entry; consumers
%%% (replayer, audit tooling) read the keys they care about.
-module(wal).

-include("wal.hrl").

-export([write/3, write/4, write/5,
         append/3, append/4, append/5,
         delete/2, delete/3, delete/4,
         read/1, read/2,
         pending/0, pending/1,
         pending_count/0, pending_count/1,
         advance_watermark/1, advance_watermark/2,
         replay/1, replay/2]).

-type server_ref() :: atom() | pid() | {atom(), node()}
                    | {global, term()} | {via, atom(), term()}.

%%% ===================================================================
%%% write
%%% ===================================================================

%% @doc Write (overwrite) a file. The content replaces whatever was there.
-spec write(binary(), binary(), binary()) ->
    ok | {busy, pos_integer()} | {error, term()}.
write(Path, Content, Origin) ->
    write(Path, Content, Origin, #{}).

-spec write(binary(), binary(), binary(), map()) ->
    ok | {busy, pos_integer()} | {error, term()}.
write(Path, Content, Origin, Meta) ->
    wal_writer:append_entry(write, Path, Content, Origin, Meta).

-spec write(server_ref(), binary(), binary(), binary(), map()) ->
    ok | {busy, pos_integer()} | {error, term()}.
write(ServerRef, Path, Content, Origin, Meta) ->
    wal_writer:append_entry(ServerRef, write, Path, Content, Origin, Meta).

%%% ===================================================================
%%% append
%%% ===================================================================

%% @doc Append content to a file (e.g. CSV row).
-spec append(binary(), binary(), binary()) ->
    ok | {busy, pos_integer()} | {error, term()}.
append(Path, Content, Origin) ->
    append(Path, Content, Origin, #{}).

-spec append(binary(), binary(), binary(), map()) ->
    ok | {busy, pos_integer()} | {error, term()}.
append(Path, Content, Origin, Meta) ->
    wal_writer:append_entry(append, Path, Content, Origin, Meta).

-spec append(server_ref(), binary(), binary(), binary(), map()) ->
    ok | {busy, pos_integer()} | {error, term()}.
append(ServerRef, Path, Content, Origin, Meta) ->
    wal_writer:append_entry(ServerRef, append, Path, Content, Origin, Meta).

%%% ===================================================================
%%% delete
%%% ===================================================================

%% @doc Delete a file.
-spec delete(binary(), binary()) ->
    ok | {busy, pos_integer()} | {error, term()}.
delete(Path, Origin) ->
    delete(Path, Origin, #{}).

-spec delete(binary(), binary(), map()) ->
    ok | {busy, pos_integer()} | {error, term()}.
delete(Path, Origin, Meta) ->
    wal_writer:append_entry(delete, Path, <<>>, Origin, Meta).

-spec delete(server_ref(), binary(), binary(), map()) ->
    ok | {busy, pos_integer()} | {error, term()}.
delete(ServerRef, Path, Origin, Meta) ->
    wal_writer:append_entry(ServerRef, delete, Path, <<>>, Origin, Meta).

%%% ===================================================================
%%% read / pending / watermark
%%% ===================================================================

%% @doc Read the current content for a path from the WAL.
-spec read(binary()) -> {ok, binary()} | {error, not_found}.
read(Path) ->
    wal_writer:read_path(Path).

-spec read(server_ref(), binary()) -> {ok, binary()} | {error, not_found}.
read(ServerRef, Path) ->
    wal_writer:read_path(ServerRef, Path).

%% @doc Return all entries with ID > watermark (uncommitted).
-spec pending() -> [#wal_entry{}].
pending() ->
    wal_writer:pending().

-spec pending(server_ref()) -> [#wal_entry{}].
pending(ServerRef) ->
    wal_writer:pending(ServerRef).

%% @doc Count of uncommitted entries.
-spec pending_count() -> non_neg_integer().
pending_count() ->
    length(pending()).

-spec pending_count(server_ref()) -> non_neg_integer().
pending_count(ServerRef) ->
    length(pending(ServerRef)).

%% @doc Advance the watermark after a successful git push.
-spec advance_watermark(non_neg_integer()) -> ok.
advance_watermark(Id) ->
    wal_writer:advance_watermark(Id).

-spec advance_watermark(server_ref(), non_neg_integer()) -> ok.
advance_watermark(ServerRef, Id) ->
    wal_writer:advance_watermark(ServerRef, Id).

%%% ===================================================================
%%% replay
%%% ===================================================================

%% @doc Replay uncommitted entries from the legacy global instance to
%%      the given data directory.
-spec replay(binary()) -> {ok, non_neg_integer()} | {error, term()}.
replay(DataDir) ->
    wal_replayer:replay(DataDir).

%% @doc Replay uncommitted entries from a specific writer instance to a
%%      data directory (used when multiple writers run side by side).
-spec replay(server_ref(), binary()) -> {ok, non_neg_integer()} | {error, term()}.
replay(ServerRef, DataDir) ->
    wal_replayer:replay(ServerRef, DataDir).
