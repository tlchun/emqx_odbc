%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. 4ζ 2021 δΈε1:15
%%%-------------------------------------------------------------------
-module(odbc_app).
-author("root").

-module(odbc_app).
-export([start/2, stop/1]).

start(_Type, Name) ->
  supervisor:start_link({local, odbc_sup}, odbc_sup,[Name]).

stop([]) -> ok.
