using System;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Security.Principal;
using Microsoft.SqlServer.Server;

public static class Command
{
    #region - Private Methods - 

    /// <summary>
    /// Use this to validate SQL server name, database name, application name
    /// </summary>
    /// <param name="name">This can be application, server or database name</param>
    /// <returns></returns>
    private static bool IsValidName(SqlString name)
    {
        if (name.IsNull)
        {
            return false;
        }

        string value = name.Value;

        // quickly check and fail if name contains "=" or ";" that causes invalid connection string
        if (string.IsNullOrWhiteSpace(value) || value.IndexOf('=') > -1 || value.IndexOf(';') > -1)
        {
            return false;
        }

        return true;
    }

    /// <summary>
    /// Use this to validate command text
    /// </summary>
    /// <param name="command">SQL CommandText or Stored Procedure</param>
    /// <returns></returns>
    private static bool IsValidCommand(SqlString command)
    {
        return !command.IsNull && !string.IsNullOrWhiteSpace(command.Value);
    }

    /// <summary>
    /// Validates application, server, database and command passed to execute
    /// </summary>
    /// <param name="application">Application name</param>
    /// <param name="server">SQL server name</param>
    /// <param name="database">Database name</param>
    /// <param name="command">Command text or Stored Procedure</param>
    /// <param name="exception">Exception output</param>
    /// <returns>true if has all valid arguments passed; other wise false</returns>
    private static bool HasValidArguments(SqlString application, SqlString server, SqlString database, SqlString command, out SqlString exception)
    {
        if (!IsValidName(application))
        {
            exception = "Invalid Application";

            return false;
        }
        else if (!IsValidName(server))
        {
            exception = "Invalid Server";

            return false;
        }
        else if (!IsValidName(database))
        {
            exception = "Invalid Database";

            return false;
        }
        else if (!IsValidCommand(command))
        {
            exception = "Invalid Command";

            return false;
        }
        else
        {
            exception = SqlString.Null;

            return true;
        }
    }

    /// <summary>
    /// Gets WindowsIdentity instance representing the Windows identity of the caller;
    /// If the client was authenticated using SQL Server Authentication then throws an exception.
    /// We do not want to support for sql logins.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    private static WindowsIdentity GetContextWindowsIdentity()
    {
        WindowsIdentity identity = SqlContext.WindowsIdentity;

        return identity ?? throw new Exception("Client is authenticated SQL Login. Cannot use current security context.");
    }
    
    /// <summary>
    /// Gets a SQL Server connection string with SSPI / Integrated Security enforced
    /// </summary>
    /// <param name="application">Application Name</param>
    /// <param name="server">Server Name</param>
    /// <param name="database">Database Name</param>
    /// <returns></returns>
    private static string GetConnectionString(SqlString application, SqlString server, SqlString database)
    {
        return $"Data Source={server.Value};Database={database.Value};Integrated Security=SSPI;Enlist=False;Application Name={application.Value};";
    }

    /// <summary>
    /// Executes commands and streams the result set back to context pipe
    /// </summary>
    /// <param name="application">Application Name</param>
    /// <param name="server">Server Name</param>
    /// <param name="database">Database Name</param>
    /// <param name="command">Command Text or Stored Procedure</param>
    /// <param name="commandTimeout">Command Timeout (optional)</param>
    /// <param name="context">Security context</param>
    private static void ExecAndSend(SqlString application, SqlString server, SqlString database, SqlString command, SqlInt32 commandTimeout, WindowsImpersonationContext context)
    {
        using (SqlConnection conn = new SqlConnection(GetConnectionString(application, server, database)))
        {
            conn.Open();

            using (SqlCommand cmd = new SqlCommand(command.Value, conn))
            {
                if (!commandTimeout.IsNull)
                {
                    cmd.CommandTimeout = commandTimeout.Value;
                }

                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    // it is important to undo context impernated for best performance & stay align with SQL CLR security context
                    context?.Undo();

                    SqlContext.Pipe.Send(reader);
                }
            }

            conn.Close();
        }
    }

    /// <summary>
    /// Executes command non-query
    /// </summary>
    /// <param name="application">Application Name</param>
    /// <param name="server">Server Name</param>
    /// <param name="database">Database Name</param>
    /// <param name="command">Command Text or Stored Procedure</param>
    /// <param name="commandTimeout">Command Timeout (optional)</param>
    private static void ExecNonQuery(SqlString application, SqlString server, SqlString database, SqlString command, SqlInt32 commandTimeout)
    {
        using (SqlConnection conn = new SqlConnection(GetConnectionString(application, server, database)))
        {
            conn.Open();

            using (SqlCommand cmd = new SqlCommand(command.Value, conn))
            {
                if (!commandTimeout.IsNull)
                {
                    cmd.CommandTimeout = commandTimeout.Value;
                }

                _ = cmd.ExecuteNonQuery();
            }

            conn.Close();
        }
    }

    /// <summary>
    /// Executes command scalar
    /// </summary>
    /// <param name="application">Application Name</param>
    /// <param name="server">Server Name</param>
    /// <param name="database">Database Name</param>
    /// <param name="command">Command Text or Stored Procedure</param>
    /// <param name="commandTimeout">Command Timeout (optional)</param>
    /// <returns></returns>
    private static string ExecScalar(SqlString application, SqlString server, SqlString database, SqlString command, SqlInt32 commandTimeout)
    {
        object result;

        using (SqlConnection conn = new SqlConnection(GetConnectionString(application, server, database)))
        {
            conn.Open();

            using (SqlCommand cmd = new SqlCommand(command.Value, conn))
            {
                if (!commandTimeout.IsNull)
                {
                    cmd.CommandTimeout = commandTimeout.Value;
                }

                result = cmd.ExecuteScalar();
            }

            conn.Close();
        }

        return result == null || result == DBNull.Value ? null : result.ToString();
    }

    #endregion

    /// <summary>
    /// Executes SQL command on passed server database and streams the result(s) to current context pipe
    /// </summary>
    /// <param name="Application">Application Name</param>
    /// <param name="Server">Server Name</param>
    /// <param name="Database">Database Name</param>
    /// <param name="Command">Command Text or Stored Procedure</param>
    /// <param name="CommandTimeout">Command Timeout (optional)</param>
    /// <param name="CurrentContext">Whethr or not to use current user context; if this is passed as false then the code executes under SQL Server service account context</param>
    /// <param name="Exception">Exception output</param>
    [SqlProcedure]
    public static void ExecuteCommand([SqlFacet(MaxSize = 128)] SqlString Application, [SqlFacet(MaxSize = 128)] SqlString Server, [SqlFacet(MaxSize = 128)] SqlString Database, [SqlFacet(MaxSize = -1)] SqlString Command, SqlInt32 CommandTimeout, SqlBoolean CurrentContext, [SqlFacet(MaxSize = -1)] out SqlString Exception)
    {
        // First make sure arguments passed are valid
        if (!HasValidArguments(Application, Server, Database, Command, out Exception))
        {
            return;
        }

        try
        {
            // If need to use current user context - this is similar to using current security context with linked server
            if (!CurrentContext.IsNull && CurrentContext.IsTrue)
            {
                // Get windows identity for current user context
                // This throws an exception if user context is using a SQL login
                WindowsIdentity identity = GetContextWindowsIdentity();

                // Now this code block needs to run under current user impersonated context
                // This makes sure to use same user authentication to remote server
                // If current user do not have access on the remote server then this fails
                using (WindowsImpersonationContext context = identity.Impersonate())
                {
                    //Execute the command on passed server database and stream results to currently executing context
                    ExecAndSend(Application, Server, Database, Command, CommandTimeout, context);
                }
            }
            // Otherwise, with no impersonation we let it use local SQL Server service account context which default
            else
            {
                //Execute the command on passed server database and stream results to currently executing context
                ExecAndSend(Application, Server, Database, Command, CommandTimeout, null);
            }
        }
        catch (Exception ex)
        {
            Exception = ex.Message;
        }
    }

    /// <summary>
    /// Executes SQL command on passed server database and does not return results
    /// </summary>
    /// <param name="Application">Application Name</param>
    /// <param name="Server">Server Name</param>
    /// <param name="Database">Database Name</param>
    /// <param name="Command">Command Text or Stored Procedure</param>
    /// <param name="CommandTimeout">Command Timeout (optional)</param>
    /// <param name="CurrentContext">Whethr or not to use current user context; if this is passed as false then the code executes under SQL Server service account context</param>
    /// <param name="Exception">Exception output</param>
    [SqlProcedure]
    public static void ExecuteNonQuery([SqlFacet(MaxSize = 128)] SqlString Application, [SqlFacet(MaxSize = 128)] SqlString Server, [SqlFacet(MaxSize = 128)] SqlString Database, [SqlFacet(MaxSize = -1)] SqlString Command, SqlInt32 CommandTimeout, SqlBoolean CurrentContext, [SqlFacet(MaxSize = -1)] out SqlString Exception)
    {
        // First make sure arguments passed are valid
        if (!HasValidArguments(Application, Server, Database, Command, out Exception))
        {
            return;
        }

        try
        {
            // If need to use current user context - this is similar to using current security context with linked server
            if (!CurrentContext.IsNull && CurrentContext.IsTrue)
            {
                // Get windows identity for current user context
                // This throws an exception if user context is using a SQL login
                WindowsIdentity identity = GetContextWindowsIdentity();

                // Now this code block needs to run under current user impersonated context
                // This makes sure to use same user authentication to remote server
                // If current user do not have access on the remote server then this fails
                using (WindowsImpersonationContext context = identity.Impersonate())
                {
                    //Execute the command on passed server database
                    ExecNonQuery(Application, Server, Database, Command, CommandTimeout);
                }
            }
            // Otherwise, with no impersonation we let it use local SQL Server service account context which default
            else
            {
                //Execute the command on passed server database
                ExecNonQuery(Application, Server, Database, Command, CommandTimeout);
            }
        }
        catch (Exception ex)
        {
            Exception = ex.Message;
        }
    }

    /// <summary>
    /// Executes SQL command on passed server database and gets first row and first column value
    /// </summary>
    /// <param name="Application">Application Name</param>
    /// <param name="Server">Server Name</param>
    /// <param name="Database">Database Name</param>
    /// <param name="Command">Command Text or Stored Procedure</param>
    /// <param name="CommandTimeout">Command Timeout (optional)</param>
    /// <param name="CurrentContext">Whethr or not to use current user context; if this is passed as false then the code executes under SQL Server service account context</param>
    /// <param name="Result"></param>
    /// <param name="Exception">Exception output</param>
    [SqlProcedure]
    public static void ExecuteScalar([SqlFacet(MaxSize = 128)] SqlString Application, [SqlFacet(MaxSize = 128)] SqlString Server, [SqlFacet(MaxSize = 128)] SqlString Database, [SqlFacet(MaxSize = -1)] SqlString Command, SqlInt32 CommandTimeout, SqlBoolean CurrentContext, [SqlFacet(MaxSize = -1)] out SqlString Result, [SqlFacet(MaxSize = -1)] out SqlString Exception)
    {
        Result = SqlString.Null;

        // First make sure arguments passed are valid
        if (!HasValidArguments(Application, Server, Database, Command, out Exception))
        {    
            return;
        }

        try
        {
            // If need to use current user context - this is similar to using current security context with linked server
            if (!CurrentContext.IsNull && CurrentContext.IsTrue)
            {
                // Get windows identity for current user context
                // This throws an exception if user context is using a SQL login
                WindowsIdentity identity = GetContextWindowsIdentity();

                // Now this code block needs to run under current user impersonated context
                // This makes sure to use same user authentication to remote server
                // If current user do not have access on the remote server then this fails
                using (WindowsImpersonationContext context = identity.Impersonate())
                {
                    //Execute the command on passed server database and gets first row - first column value
                    Result = ExecScalar(Application, Server, Database, Command, CommandTimeout);
                }
            }
            else
            {
                //Execute the command on passed server database and gets first row - first column value
                Result = ExecScalar(Application, Server, Database, Command, CommandTimeout);
            }
        }
        catch (Exception ex)
        {
            Exception = ex.Message;
        }
    }
}