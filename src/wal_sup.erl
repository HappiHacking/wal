-module(wal_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    WalDir = application:get_env(wal, wal_dir, "/data/aurora/wal"),
    Children = [
        #{id => wal_writer,
          start => {wal_writer, start_link, [WalDir]},
          restart => permanent,
          type => worker}
    ],
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, Children}}.
