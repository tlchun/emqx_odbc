{application, odbc,
  [{description, "Erlang ODBC application"},
    {vsn, "1.0.0"},
    {modules, [odbc, odbc_app, odbc_sup]},
    {registered, [odbc_sup]},
    {applications, [kernel, stdlib]},
    {env,[]},
    {mod, {odbc_app, []}}]}.

