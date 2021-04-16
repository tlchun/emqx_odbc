%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. 4月 2021 下午1:14
%%%-------------------------------------------------------------------
-module(odbc).
-author("root").

-behaviour(gen_server).

-include("../include/odbc_internal.hrl").

-export([start/0,
  start/1,
  stop/0,
  connect/2,
  disconnect/1,
  commit/2,
  commit/3,
  sql_query/2,
  sql_query/3,
  select_count/2,
  select_count/3,
  first/1,
  first/2,
  last/1,
  last/2,
  next/1,
  next/2,
  prev/1,
  prev/2,
  select/3,
  select/4,
  param_query/3,
  param_query/4,
  describe_table/2,
  describe_table/3]).

-export([start_link_sup/1]).

-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-record(state,
{erlang_port,
  reply_to,
  owner,
  result_set = undefined,
  auto_commit_mode = on,
  absolute_pos,
  relative_pos,
  scrollable_cursors,
  state = connecting,
  pending_request,
  num_timeouts = 0,
  listen_sockets,
  sup_socket,
  odbc_socket}).

start() -> application:start(odbc).

start(Type) -> application:start(odbc, Type).

stop() -> application:stop(odbc).

connect(ConnectionStr, Options)
  when is_list(ConnectionStr), is_list(Options) ->
  try supervisor:start_child(odbc_sup,
    [[{client, self()}]])
  of
    {ok, Pid} -> connect(Pid, ConnectionStr, Options);
    {error, Reason} -> {error, Reason}
  catch
    exit:{noproc, _} -> {error, odbc_not_started}
  end.

disconnect(ConnectionReference)
  when is_pid(ConnectionReference) ->
  ODBCCmd = [2],
  case call(ConnectionReference,
    {disconnect, ODBCCmd},
    5000)
  of
    {error, connection_closed} -> ok;
    ok -> ok;
    Other -> Other
  end.

commit(ConnectionReference, CommitMode) ->
  commit(ConnectionReference, CommitMode, infinity).

commit(ConnectionReference, commit, infinity)
  when is_pid(ConnectionReference) ->
  ODBCCmd = [3, 4],
  call(ConnectionReference, {commit, ODBCCmd}, infinity);
commit(ConnectionReference, commit, TimeOut)
  when is_pid(ConnectionReference), is_integer(TimeOut),
  TimeOut > 0 ->
  ODBCCmd = [3, 4],
  call(ConnectionReference, {commit, ODBCCmd}, TimeOut);
commit(ConnectionReference, rollback, infinity)
  when is_pid(ConnectionReference) ->
  ODBCCmd = [3, 5],
  call(ConnectionReference, {commit, ODBCCmd}, infinity);
commit(ConnectionReference, rollback, TimeOut)
  when is_pid(ConnectionReference), is_integer(TimeOut),
  TimeOut > 0 ->
  ODBCCmd = [3, 5],
  call(ConnectionReference, {commit, ODBCCmd}, TimeOut).

sql_query(ConnectionReference, SQLQuery) ->
  sql_query(ConnectionReference, SQLQuery, infinity).

sql_query(ConnectionReference, SQLQuery, infinity)
  when is_pid(ConnectionReference), is_list(SQLQuery) ->
  ODBCCmd = [6, SQLQuery],
  call(ConnectionReference,
    {sql_query, ODBCCmd},
    infinity);
sql_query(ConnectionReference, SQLQuery, TimeOut)
  when is_pid(ConnectionReference), is_list(SQLQuery),
  is_integer(TimeOut), TimeOut > 0 ->
  ODBCCmd = [6, SQLQuery],
  call(ConnectionReference,
    {sql_query, ODBCCmd},
    TimeOut).

select_count(ConnectionReference, SQLQuery) ->
  select_count(ConnectionReference, SQLQuery, infinity).

select_count(ConnectionReference, SQLQuery, infinity)
  when is_pid(ConnectionReference), is_list(SQLQuery) ->
  ODBCCmd = [7, SQLQuery],
  call(ConnectionReference,
    {select_count, ODBCCmd},
    infinity);
select_count(ConnectionReference, SQLQuery, TimeOut)
  when is_pid(ConnectionReference), is_list(SQLQuery),
  is_integer(TimeOut), TimeOut > 0 ->
  ODBCCmd = [7, SQLQuery],
  call(ConnectionReference,
    {select_count, ODBCCmd},
    TimeOut).

first(ConnectionReference) ->
  first(ConnectionReference, infinity).

first(ConnectionReference, infinity)
  when is_pid(ConnectionReference) ->
  ODBCCmd = [12, 8],
  call(ConnectionReference,
    {select_cmd, absolute, ODBCCmd},
    infinity);
first(ConnectionReference, TimeOut)
  when is_pid(ConnectionReference), is_integer(TimeOut),
  TimeOut > 0 ->
  ODBCCmd = [12, 8],
  call(ConnectionReference,
    {select_cmd, absolute, ODBCCmd},
    TimeOut).

last(ConnectionReference) ->
  last(ConnectionReference, infinity).

last(ConnectionReference, infinity)
  when is_pid(ConnectionReference) ->
  ODBCCmd = [12, 9],
  call(ConnectionReference,
    {select_cmd, absolute, ODBCCmd},
    infinity);
last(ConnectionReference, TimeOut)
  when is_pid(ConnectionReference), is_integer(TimeOut),
  TimeOut > 0 ->
  ODBCCmd = [12, 9],
  call(ConnectionReference,
    {select_cmd, absolute, ODBCCmd},
    TimeOut).

next(ConnectionReference) ->
  next(ConnectionReference, infinity).

next(ConnectionReference, infinity)
  when is_pid(ConnectionReference) ->
  ODBCCmd = [12, 10],
  call(ConnectionReference,
    {select_cmd, next, ODBCCmd},
    infinity);
next(ConnectionReference, TimeOut)
  when is_pid(ConnectionReference), is_integer(TimeOut),
  TimeOut > 0 ->
  ODBCCmd = [12, 10],
  call(ConnectionReference,
    {select_cmd, next, ODBCCmd},
    TimeOut).

prev(ConnectionReference) ->
  prev(ConnectionReference, infinity).

prev(ConnectionReference, infinity)
  when is_pid(ConnectionReference) ->
  ODBCCmd = [12, 11],
  call(ConnectionReference,
    {select_cmd, relative, ODBCCmd},
    infinity);
prev(ConnectionReference, TimeOut)
  when is_pid(ConnectionReference), is_integer(TimeOut),
  TimeOut > 0 ->
  ODBCCmd = [12, 11],
  call(ConnectionReference,
    {select_cmd, relative, ODBCCmd},
    TimeOut).

select(ConnectionReference, Position, N) ->
  select(ConnectionReference, Position, N, infinity).

select(ConnectionReference, next, N, infinity)
  when is_pid(ConnectionReference), is_integer(N),
  N > 0 ->
  ODBCCmd = [12,
    15,
    integer_to_list(0),
    ";",
    integer_to_list(N),
    ";"],
  call(ConnectionReference,
    {select_cmd, next, ODBCCmd},
    infinity);
select(ConnectionReference, next, N, TimeOut)
  when is_pid(ConnectionReference), is_integer(N), N > 0,
  is_integer(TimeOut), TimeOut > 0 ->
  ODBCCmd = [12,
    15,
    integer_to_list(0),
    ";",
    integer_to_list(N),
    ";"],
  call(ConnectionReference,
    {select_cmd, next, ODBCCmd},
    TimeOut);
select(ConnectionReference, {relative, Pos}, N,
    infinity)
  when is_pid(ConnectionReference), is_integer(Pos),
  Pos > 0, is_integer(N), N > 0 ->
  ODBCCmd = [12,
    13,
    integer_to_list(Pos),
    ";",
    integer_to_list(N),
    ";"],
  call(ConnectionReference,
    {select_cmd, relative, ODBCCmd},
    infinity);
select(ConnectionReference, {relative, Pos}, N, TimeOut)
  when is_pid(ConnectionReference), is_integer(Pos),
  Pos > 0, is_integer(N), N > 0, is_integer(TimeOut),
  TimeOut > 0 ->
  ODBCCmd = [12,
    13,
    integer_to_list(Pos),
    ";",
    integer_to_list(N),
    ";"],
  call(ConnectionReference,
    {select_cmd, relative, ODBCCmd},
    TimeOut);
select(ConnectionReference, {absolute, Pos}, N,
    infinity)
  when is_pid(ConnectionReference), is_integer(Pos),
  Pos > 0, is_integer(N), N > 0 ->
  ODBCCmd = [12,
    14,
    integer_to_list(Pos),
    ";",
    integer_to_list(N),
    ";"],
  call(ConnectionReference,
    {select_cmd, absolute, ODBCCmd},
    infinity);
select(ConnectionReference, {absolute, Pos}, N, TimeOut)
  when is_pid(ConnectionReference), is_integer(Pos),
  Pos > 0, is_integer(N), N > 0, is_integer(TimeOut),
  TimeOut > 0 ->
  ODBCCmd = [12,
    14,
    integer_to_list(Pos),
    ";",
    integer_to_list(N),
    ";"],
  call(ConnectionReference,
    {select_cmd, absolute, ODBCCmd},
    TimeOut).

param_query(ConnectionReference, SQLQuery, Params) ->
  param_query(ConnectionReference,
    SQLQuery,
    Params,
    infinity).

param_query(ConnectionReference, SQLQuery, Params,
    infinity)
  when is_pid(ConnectionReference), is_list(SQLQuery),
  is_list(Params) ->
  Values = param_values(Params),
  NoRows = length(Values),
  NewParams = lists:map(fun fix_params/1, Params),
  ODBCCmd = [16,
    term_to_binary({SQLQuery ++ [0], NoRows, NewParams})],
  call(ConnectionReference,
    {param_query, ODBCCmd},
    infinity);
param_query(ConnectionReference, SQLQuery, Params,
    TimeOut)
  when is_pid(ConnectionReference), is_list(SQLQuery),
  is_list(Params), is_integer(TimeOut), TimeOut > 0 ->
  Values = param_values(Params),
  NoRows = length(Values),
  NewParams = lists:map(fun fix_params/1, Params),
  ODBCCmd = [16,
    term_to_binary({SQLQuery ++ [0], NoRows, NewParams})],
  call(ConnectionReference,
    {param_query, ODBCCmd},
    TimeOut).

describe_table(ConnectionReference, Table) ->
  describe_table(ConnectionReference, Table, infinity).

describe_table(ConnectionReference, Table, infinity)
  when is_pid(ConnectionReference), is_list(Table) ->
  ODBCCmd = [17, "SELECT * FROM " ++ Table],
  call(ConnectionReference,
    {describe_table, ODBCCmd},
    infinity);
describe_table(ConnectionReference, Table, TimeOut)
  when is_pid(ConnectionReference), is_list(Table),
  is_integer(TimeOut), TimeOut > 0 ->
  ODBCCmd = [17, "SELECT * FROM " ++ Table],
  call(ConnectionReference,
    {describe_table, ODBCCmd},
    TimeOut).

start_link_sup(Args) ->
  gen_server:start_link(odbc, Args, []).

init(Args) ->
  process_flag(trap_exit, true),
  {value, {client, ClientPid}} = lists:keysearch(client,
    1,
    Args),
  erlang:monitor(process, ClientPid),
  Inet = case gen_tcp:listen(0, [inet6, {ip, loopback}])
         of
           {ok, Dummyport} ->
             gen_tcp:close(Dummyport),
             inet6;
           _ -> inet
         end,
  {ok, ListenSocketSup} = gen_tcp:listen(0,
    [Inet,
      binary,
      {packet, 4},
      {active, false},
      {nodelay, true},
      {ip, loopback}]),
  {ok, ListenSocketOdbc} = gen_tcp:listen(0,
    [Inet,
      binary,
      {packet, 4},
      {active, false},
      {nodelay, true},
      {ip, loopback}]),
  case os:find_executable("odbcserver",
    filename:nativename(filename:join(code:priv_dir(odbc),
      "bin")))
  of
    FileName when is_list(FileName) ->
      Port = open_port({spawn, "\"" ++ FileName ++ "\""},
        [{packet, 4}, binary, exit_status]),
      State = #state{listen_sockets =
      [ListenSocketSup, ListenSocketOdbc],
        erlang_port = Port, owner = ClientPid},
      {ok, State};
    false -> {stop, port_program_executable_not_found}
  end.

handle_call({Client, Msg, Timeout}, From,
    State = #state{owner = Client, reply_to = undefined}) ->
  handle_msg(Msg, Timeout, State#state{reply_to = From});
handle_call(Request = {Client, _, Timeout}, From,
    State = #state{owner = Client, reply_to = skip,
      num_timeouts = N})
  when N < 10 ->
  {noreply,
    State#state{pending_request = {Request, From}},
    Timeout};
handle_call({Client, _, _}, From,
    State = #state{owner = Client, num_timeouts = N})
  when N >= 10 ->
  gen_server:reply(From, {error, connection_closed}),
  {stop,
    too_many_sequential_timeouts,
    State#state{reply_to = undefined}};
handle_call(_, _, State) ->
  {reply,
    {error, process_not_owner_of_odbc_connection},
    State#state{reply_to = undefined}}.

handle_msg({connect,
  ODBCCmd,
  AutoCommitMode,
  SrollableCursors},
    Timeout, State) ->
  [ListenSocketSup, ListenSocketOdbc] =
    State#state.listen_sockets,
  {ok, InetPortSup} = inet:port(ListenSocketSup),
  {ok, InetPortOdbc} = inet:port(ListenSocketOdbc),
  port_command(State#state.erlang_port,
    [integer_to_list(InetPortSup),
      ";",
      integer_to_list(InetPortOdbc),
      0]),
  NewState = State#state{auto_commit_mode =
  AutoCommitMode,
    scrollable_cursors = SrollableCursors},
  case gen_tcp:accept(ListenSocketSup, port_timeout()) of
    {ok, SupSocket} ->
      gen_tcp:close(ListenSocketSup),
      case gen_tcp:accept(ListenSocketOdbc, port_timeout()) of
        {ok, OdbcSocket} ->
          gen_tcp:close(ListenSocketOdbc),
          odbc_send(OdbcSocket, ODBCCmd),
          {noreply,
            NewState#state{odbc_socket = OdbcSocket,
              sup_socket = SupSocket},
            Timeout};
        {error, Reason} ->
          {stop, Reason, {error, connection_closed}, NewState}
      end;
    {error, Reason} ->
      {stop, Reason, {error, connection_closed}, NewState}
  end;
handle_msg({disconnect, ODBCCmd}, Timeout, State) ->
  odbc_send(State#state.odbc_socket, ODBCCmd),
  {noreply, State#state{state = disconnecting}, Timeout};
handle_msg({commit, _ODBCCmd}, Timeout,
    State = #state{auto_commit_mode = on}) ->
  {reply,
    {error, not_an_explicit_commit_connection},
    State#state{reply_to = undefined},
    Timeout};
handle_msg({commit, ODBCCmd}, Timeout,
    State = #state{auto_commit_mode = off}) ->
  odbc_send(State#state.odbc_socket, ODBCCmd),
  {noreply, State, Timeout};
handle_msg({sql_query, ODBCCmd}, Timeout, State) ->
  odbc_send(State#state.odbc_socket, ODBCCmd),
  {noreply, State#state{result_set = undefined}, Timeout};
handle_msg({param_query, ODBCCmd}, Timeout, State) ->
  odbc_send(State#state.odbc_socket, ODBCCmd),
  {noreply, State#state{result_set = undefined}, Timeout};
handle_msg({describe_table, ODBCCmd}, Timeout, State) ->
  odbc_send(State#state.odbc_socket, ODBCCmd),
  {noreply, State#state{result_set = undefined}, Timeout};
handle_msg({select_count, ODBCCmd}, Timeout, State) ->
  odbc_send(State#state.odbc_socket, ODBCCmd),
  {noreply, State#state{result_set = exists}, Timeout};
handle_msg({select_cmd, absolute, ODBCCmd}, Timeout,
    State = #state{result_set = exists,
      absolute_pos = true}) ->
  odbc_send(State#state.odbc_socket, ODBCCmd),
  {noreply, State, Timeout};
handle_msg({select_cmd, relative, ODBCCmd}, Timeout,
    State = #state{result_set = exists,
      relative_pos = true}) ->
  odbc_send(State#state.odbc_socket, ODBCCmd),
  {noreply, State, Timeout};
handle_msg({select_cmd, next, ODBCCmd}, Timeout,
    State = #state{result_set = exists}) ->
  odbc_send(State#state.odbc_socket, ODBCCmd),
  {noreply, State, Timeout};
handle_msg({select_cmd, _Type, _ODBCCmd}, _Timeout,
    State = #state{result_set = undefined}) ->
  {reply,
    {error, result_set_does_not_exist},
    State#state{reply_to = undefined}};
handle_msg({select_cmd, _Type, _ODBCCmd}, _Timeout,
    State) ->
  Reply = case State#state.scrollable_cursors of
            on -> {error, driver_does_not_support_function};
            off -> {error, scrollable_cursors_disabled}
          end,
  {reply, Reply, State#state{reply_to = undefined}};
handle_msg(Request, _Timeout, State) ->
  {stop,
    {'API_violation_connection_colsed', Request},
    {error, connection_closed},
    State#state{reply_to = undefined}}.

handle_cast(Msg, State) ->
  {stop, {'API_violation_connection_colsed', Msg}, State}.

handle_info({tcp, Socket, BinData},
    State = #state{state = connecting, reply_to = From,
      odbc_socket = Socket}) ->
  case binary_to_term(BinData) of
    {ok, AbsolutSupport, RelativeSupport} ->
      NewState = State#state{absolute_pos = AbsolutSupport,
        relative_pos = RelativeSupport},
      gen_server:reply(From, ok),
      {noreply,
        NewState#state{state = connected,
          reply_to = undefined}};
    Error ->
      gen_server:reply(From, Error),
      {stop, normal, State#state{reply_to = undefined}}
  end;
handle_info({tcp, Socket, _},
    State = #state{state = connected, odbc_socket = Socket,
      reply_to = skip, pending_request = undefined}) ->
  {noreply, State#state{reply_to = undefined}};
handle_info({tcp, Socket, _},
    State = #state{state = connected, odbc_socket = Socket,
      reply_to = skip}) ->
  {{_, Msg, Timeout}, From} = State#state.pending_request,
  handle_msg(Msg,
    Timeout,
    State#state{pending_request = undefined,
      reply_to = From});
handle_info({tcp, Socket, BinData},
    State = #state{state = connected, reply_to = From,
      odbc_socket = Socket}) ->
  gen_server:reply(From, BinData),
  {noreply,
    State#state{reply_to = undefined, num_timeouts = 0}};
handle_info({tcp, Socket, BinData},
    State = #state{state = disconnecting, reply_to = From,
      odbc_socket = Socket}) ->
  gen_server:reply(From, ok),
  case binary_to_term(BinData) of
    ok -> ok;
    {error, Reason} ->
      Report =
        io_lib:format("ODBC could not end connection gracefully "
        "due to ~p~n",
          [Reason]),
      error_logger:error_report(Report)
  end,
  {stop, normal, State#state{reply_to = undefined}};
handle_info(timeout,
    State = #state{state = disconnecting, reply_to = From})
  when From /= undefined ->
  gen_server:reply(From, ok),
  {stop,
    {timeout,
      "Port program is not responding to disconnect, "
      "will be killed"},
    State};
handle_info(timeout,
    State = #state{state = connecting, reply_to = From})
  when From /= undefined ->
  gen_server:reply(From, timeout),
  {stop, normal, State#state{reply_to = undefined}};
handle_info(timeout,
    State = #state{state = connected,
      pending_request = undefined, reply_to = From})
  when From /= undefined ->
  gen_server:reply(From, timeout),
  {noreply,
    State#state{reply_to = skip,
      num_timeouts = State#state.num_timeouts + 1}};
handle_info(timeout,
    State = #state{state = connected,
      pending_request =
      {{_, {disconnect, _}, _}, PendingFrom}}) ->
  gen_server:reply(PendingFrom, ok),
  {stop,
    {timeout,
      "Port-program busy when trying to disconnect, "
      " will be killed"},
    State#state{pending_request = undefined,
      reply_to = undefined,
      num_timeouts = State#state.num_timeouts + 1}};
handle_info(timeout,
    State = #state{state = connected,
      pending_request = {_, PendingFrom}}) ->
  gen_server:reply(PendingFrom, timeout),
  {noreply,
    State#state{pending_request = undefined,
      num_timeouts = State#state.num_timeouts + 1}};
handle_info({Port, {exit_status, 0}},
    State = #state{erlang_port = Port,
      state = disconnecting}) ->
  {noreply, State};
handle_info({Port, {exit_status, Status}},
    State = #state{erlang_port = Port}) ->
  {stop,
    {port_exit,
      fun (0) -> normal_exit;
        (1) -> abnormal_exit;
        (2) -> memory_allocation_failed;
        (3) -> setting_of_environment_attributes_failed;
        (4) -> setting_of_connection_attributes_faild;
        (5) -> freeing_of_memory_failed;
        (6) -> receiving_port_message_header_failed;
        (7) -> receiving_port_message_body_failed;
        (8) -> retrieving_of_binary_data_failed;
        (9) -> failed_to_create_thread;
        (10) -> does_not_support_param_arrays;
        (11) -> too_old_verion_of_winsock;
        (12) -> socket_connect_failed;
        (13) -> socket_send_message_header_failed;
        (14) -> socket_send_message_body_failed;
        (15) -> socket_received_too_large_message;
        (16) -> too_large_message_in_socket_send;
        (17) -> socket_receive_message_header_failed;
        (18) -> socket_receive_message_body_failed;
        (19) -> could_not_access_column_count;
        (20) -> could_not_access_row_count;
        (21) -> could_not_access_table_description;
        (22) -> could_not_bind_data_buffers;
        (23) -> collecting_of_driver_information_faild;
        (_) -> killed
      end(Status)},
    State};
handle_info({'EXIT', Port, _},
    State = #state{erlang_port = Port,
      state = disconnecting}) ->
  {noreply, State};
handle_info({'EXIT', Port, Reason},
    State = #state{erlang_port = Port}) ->
  {stop, Reason, State};
handle_info({'DOWN', _Ref, _Type, _Process, normal},
    State) ->
  {stop, normal, State#state{reply_to = undefined}};
handle_info({'DOWN', _Ref, _Type, _Process, timeout},
    State) ->
  {stop, normal, State#state{reply_to = undefined}};
handle_info({'DOWN', _Ref, _Type, _Process, shutdown},
    State) ->
  {stop, normal, State#state{reply_to = undefined}};
handle_info({'DOWN', _Ref, _Type, Process, Reason},
    State) ->
  {stop,
    {stopped, {'EXIT', Process, Reason}},
    State#state{reply_to = undefined}};
handle_info({tcp_closed, Socket},
    State = #state{odbc_socket = Socket,
      state = disconnecting}) ->
  {stop, normal, State};
handle_info(Info, State) ->
  Report =
    io_lib:format("ODBC: received unexpected info: ~p~n",
      [Info]),
  error_logger:error_report(Report),
  {noreply, State}.

terminate({port_exit, _Reason},
    State = #state{reply_to = undefined}) ->
  gen_tcp:close(State#state.odbc_socket),
  gen_tcp:close(State#state.sup_socket),
  ok;
terminate(_Reason,
    State = #state{reply_to = undefined}) ->
  catch gen_tcp:send(State#state.sup_socket, [18, 0]),
  catch gen_tcp:close(State#state.odbc_socket),
  catch gen_tcp:close(State#state.sup_socket),
  catch port_close(State#state.erlang_port),
  ok;
terminate(Reason, State = #state{reply_to = From}) ->
  gen_server:reply(From, {error, connection_closed}),
  terminate(Reason, State#state{reply_to = undefined}).

code_change(_Vsn, State, _Extra) -> {ok, State}.

connect(ConnectionReferense, ConnectionStr, Options) ->
  {C_AutoCommitMode, ERL_AutoCommitMode} =
    connection_config(auto_commit, Options),
  TimeOut = connection_config(timeout, Options),
  {C_TraceDriver, _} = connection_config(trace_driver,
    Options),
  {C_SrollableCursors, ERL_SrollableCursors} =
    connection_config(scrollable_cursors, Options),
  {C_TupleRow, _} = connection_config(tuple_row, Options),
  {BinaryStrings, _} = connection_config(binary_strings,
    Options),
  {ExtendedErrors, _} = connection_config(extended_errors,
    Options),
  ODBCCmd = [1,
    C_AutoCommitMode,
    C_TraceDriver,
    C_SrollableCursors,
    C_TupleRow,
    BinaryStrings,
    ExtendedErrors,
    ConnectionStr],
  case call(ConnectionReferense,
    {connect,
      ODBCCmd,
      ERL_AutoCommitMode,
      ERL_SrollableCursors},
    TimeOut)
  of
    ok -> {ok, ConnectionReferense};
    Error -> Error
  end.

odbc_send(Socket, Msg) ->
  NewMsg = Msg ++ [0],
  ok = gen_tcp:send(Socket, NewMsg),
  ok = inet:setopts(Socket, [{active, once}]).

connection_config(Key, Options) ->
  case lists:keysearch(Key, 1, Options) of
    {value, {Key, on}} -> {1, on};
    {value, {Key, off}} -> {2, off};
    {value, {Key, Value}} -> Value;
    _ -> connection_default(Key)
  end.

connection_default(auto_commit) -> {1, on};
connection_default(timeout) -> infinity;
connection_default(tuple_row) -> {1, on};
connection_default(trace_driver) -> {2, off};
connection_default(scrollable_cursors) -> {1, on};
connection_default(binary_strings) -> {2, off};
connection_default(extended_errors) -> {2, off}.

call(ConnectionReference, Msg, Timeout) ->
  Result = (catch gen_server:call(ConnectionReference,
    {self(), Msg, Timeout},
    infinity)),
  case Result of
    Binary when is_binary(Binary) -> decode(Binary);
    timeout -> exit(timeout);
    {'EXIT', _} -> {error, connection_closed};
    Term -> Term
  end.

decode(Binary) ->
  case binary_to_term(Binary) of
    [ResultSet] -> ResultSet;
    param_badarg ->
      exit({badarg, odbc, param_query, 'Params'});
    MultipleResultSets_or_Other ->
      MultipleResultSets_or_Other
  end.

param_values(Params) ->
  case Params of
    [{_, Values} | _] -> Values;
    [{_, _, Values} | _] -> Values;
    [] -> []
  end.

fix_params({sql_integer, InOut, Values}) ->
  {2, fix_inout(InOut), [256 | Values]};
fix_params({sql_smallint, InOut, Values}) ->
  {1, fix_inout(InOut), [256 | Values]};
fix_params({sql_tinyint, InOut, Values}) ->
  {11, fix_inout(InOut), [256 | Values]};
fix_params({{sql_decimal, Precision, 0}, InOut, Values})
  when Precision >= 0, Precision =< 9 ->
  {3, Precision, 0, fix_inout(InOut), [256 | Values]};
fix_params({{sql_decimal, Precision, Scale},
  InOut,
  Values}) ->
  {3, Precision, Scale, fix_inout(InOut), Values};
fix_params({{sql_numeric, Precision, 0}, InOut, Values})
  when Precision >= 0, Precision =< 9 ->
  {4, Precision, 0, fix_inout(InOut), [256 | Values]};
fix_params({{sql_numeric, Precision, Scale},
  InOut,
  Values}) ->
  {4, Precision, Scale, fix_inout(InOut), Values};
fix_params({{sql_char, Max}, InOut, Values}) ->
  NewValues = string_terminate(Values),
  {5, Max, fix_inout(InOut), NewValues};
fix_params({{sql_varchar, Max}, InOut, Values}) ->
  NewValues = string_terminate(Values),
  {6, Max, fix_inout(InOut), NewValues};
fix_params({{sql_wchar, Max}, InOut, Values}) ->
  NewValues = string_terminate(Values),
  {12, Max, fix_inout(InOut), NewValues};
fix_params({{sql_wvarchar, Max}, InOut, Values}) ->
  NewValues = string_terminate(Values),
  {13, Max, fix_inout(InOut), NewValues};
fix_params({{sql_wlongvarchar, Max}, InOut, Values}) ->
  NewValues = string_terminate(Values),
  {15, Max, fix_inout(InOut), NewValues};
fix_params({{sql_float, Precision}, InOut, Values}) ->
  {7, Precision, fix_inout(InOut), Values};
fix_params({sql_real, InOut, Values}) ->
  {8, fix_inout(InOut), Values};
fix_params({sql_double, InOut, Values}) ->
  {9, fix_inout(InOut), Values};
fix_params({sql_bit, InOut, Values}) ->
  {10, fix_inout(InOut), Values};
fix_params({sql_timestamp, InOut, Values}) ->
  NewValues = case catch lists:map(fun ({{Year,
    Month,
    Day},
    {Hour, Minute, Second}}) ->
    {Year,
      Month,
      Day,
      Hour,
      Minute,
      Second};
    (null) -> null
                                   end,
    Values)
              of
                Result -> Result
              end,
  {14, fix_inout(InOut), NewValues};
fix_params({Type, Values}) ->
  fix_params({Type, in, Values}).

fix_inout(in) -> 0;
fix_inout(out) -> 1;
fix_inout(inout) -> 2.

string_terminate(Values) ->
  case catch lists:map(fun string_terminate_value/1,
    Values)
  of
    Result -> Result
  end.

string_terminate_value(String) when is_list(String) ->
  String ++ [0];
string_terminate_value(Binary) when is_binary(Binary) ->
  <<Binary/binary, 0:16>>;
string_terminate_value(null) -> null.

port_timeout() ->
  application:get_env(odbc, port_timeout, 5000).
