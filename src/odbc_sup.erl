%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. 4月 2021 下午1:15
%%%-------------------------------------------------------------------
-module(odbc_sup).
-author("root").

-behaviour(supervisor).

-export([init/1]).

init([Name]) ->
  RestartStrategy = simple_one_for_one,
  MaxR = 0,
  MaxT = 3600,
  StartFunc = {odbc, start_link_sup, []},
  Restart = temporary,
  Shutdown = 7000,
  Modules = [odbc],
  Type = worker,
  ChildSpec = {Name,
    StartFunc,
    Restart,
    Shutdown,
    Type,
    Modules},
  {ok, {{RestartStrategy, MaxR, MaxT}, [ChildSpec]}}.

