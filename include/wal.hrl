-record(wal_entry, {
    id       :: non_neg_integer(),
    ts       :: binary(),            %% ISO8601
    op       :: write | append | delete,
    path     :: binary(),            %% relative to data_dir
    content  :: binary(),            %% file content (empty for delete)
    origin   :: binary(),            %% who wrote it
    meta     :: map()                %% optional context
}).
