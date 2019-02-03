// © 2016 Sitecore Corporation A/S. All rights reserved.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using Sitecore.Diagnostics;
using Sitecore.ExM.Framework.Data;
using Sitecore.ExM.Framework.Distributed.Tasks;
using ConfigurationException = Sitecore.Exceptions.ConfigurationException;

namespace Sitecore.Support.ExM.Framework.DataProviders
{
  /// <summary>
  /// An implementation of <see cref="IDatabaseTaskProvider"/> for an SQL database.
  /// </summary>
  public class SqlDatabaseTaskProvider : IDatabaseTaskProvider
  {
    /// <summary>
    /// Contains the connection string used by this instance.
    /// </summary>
    [NotNull]
    private readonly string _connectionString;

    /// <summary>
    /// The name of the database table.
    /// </summary>
    [NotNull]
    private readonly string _tableName;

    /// <summary>
    /// The number of attempts to execute a request if it fails due to a deadlock.
    /// </summary>
    private int _numDeadlockAttempts = 3;

    /// <summary>
    /// Number of milliseconds for command timeout
    /// </summary>
    private readonly int commandTimeout;

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlDatabaseTaskProvider"/> class.
    /// </summary>
    /// <param name="connectionStringName">The name of the connection string to be used by this instance.</param>
    /// <param name="tableName">The name of the database table to be used by this instance.</param>
    /// <exception cref="Sitecore.Exceptions.ConfigurationException">
    /// Thrown if the specified connection string could not be found.
    /// </exception>
    public SqlDatabaseTaskProvider([NotNull] string connectionStringName, [NotNull] string tableName, string commandTimeout)
    {
      Assert.ArgumentNotNull(connectionStringName, "connectionStringName");
      Assert.ArgumentNotNullOrEmpty(tableName, "tableName");

      var connectionStringSettings = ConfigurationManager.ConnectionStrings[connectionStringName];

      if (connectionStringSettings == null)
      {
        throw new ConfigurationException(string.Format("No connection string configuration was found by the name '{0}'.", connectionStringName));
      }

      _connectionString = connectionStringSettings.ConnectionString;
      _tableName = tableName;

      // The timeout value from the configuration (the "commandTimeout" param)
      this.commandTimeout = int.Parse(commandTimeout);
    }

    /// <summary>
    /// Gets or sets the number of attempts to execute a request if it fails due to a deadlock.
    /// </summary>
    [UsedImplicitly]
    public int NumDeadlockAttempts
    {
      get { return _numDeadlockAttempts; }

      set
      {
        Assert.ArgumentCondition(value > 0, "value", "The number of attempts to execute request that fails due to a deadlock must be positive.");
        _numDeadlockAttempts = value;
      }
    }

    /// <summary>
    /// Inserts a collection of new tasks in the database.
    /// </summary>
    /// <param name="tasks">The tasks to be inserted.</param>
    public void InsertTasks(IEnumerable<DatabaseTaskDto> tasks)
    {
      Assert.ArgumentNotNull(tasks, "tasks");

      // Convert the tasks to a data table.
      var tasksTable = GetTasksDtoTable();

      foreach (var task in tasks)
      {
        tasksTable.Rows.Add(task.TaskId.Value, task.TaskPoolId, task.StateId, task.Timestamp, task.TaskData);
      }

      // Insert the tasks.
      SqlUtil.InvokeWithDeadlockRetry(
          () =>
          {
            using (var connection = new SqlConnection(_connectionString))
            {
              connection.Open();
              using (var command = connection.CreateCommand())
              {
                command.CommandText = string.Format(
                          "INSERT INTO [{0}] ([TaskID],[TaskPoolID],[StateID],[Timestamp],[Generation],[TaskData]) "
                              + "SELECT [TaskID],[TaskPoolID],[StateID],[Timestamp],0,[TaskData] FROM @Tasks",
                          _tableName);
                command.CommandTimeout = commandTimeout;
                var sessionIdsParam = command.Parameters.AddWithValue("@Tasks", tasksTable);
                sessionIdsParam.SqlDbType = SqlDbType.Structured;
                sessionIdsParam.TypeName = tasksTable.TableName;

                command.ExecuteNonQuery();
              }
            }
          },
          NumDeadlockAttempts);
    }

    /// <summary>
    /// Removes a set of task records from the database.
    /// </summary>
    /// <param name="taskIds">The set of ids of tasks to remove.</param>
    public void RemoveTasks(IEnumerable<BinaryId48> taskIds)
    {
      Assert.ArgumentNotNull(taskIds, "taskIds");

      // Convert the task ids to a data table.
      var taskIdsTable = GetTasksIdsTable();

      foreach (var taskId in taskIds)
      {
        taskIdsTable.Rows.Add(taskId.Value);
      }

      // Remove the tasks.
      SqlUtil.InvokeWithDeadlockRetry(
          () =>
          {
            using (var connection = new SqlConnection(_connectionString))
            {
              connection.Open();
              using (var command = connection.CreateCommand())
              {
                command.CommandText = string.Format("DELETE FROM [{0}] WHERE [TaskID] IN (SELECT [TaskID] FROM @TaskIDs)", _tableName);

                command.CommandTimeout = commandTimeout;
                var sessionIdsParam = command.Parameters.AddWithValue("@TaskIDs", taskIdsTable);
                sessionIdsParam.SqlDbType = SqlDbType.Structured;
                sessionIdsParam.TypeName = taskIdsTable.TableName;

                command.ExecuteNonQuery();
              }
            }
          },
          NumDeadlockAttempts);
    }

    /// <summary>
    /// Updates and retrieves a set of task records in one operation.
    /// </summary>
    /// <param name="count">The number of task records to retrieve.</param>
    /// <param name="taskPoolId">The id of the pool to retrieve task records from.</param>
    /// <param name="stateId">The state id of the task records to update and retrieve.</param>
    /// <param name="updatedStateId">A new state id to assign to the updated task records.</param>
    /// <param name="upperTimestampLimit">An upper limit of the timestamp of the task records to update and retrieve..</param>
    /// <param name="updatedTimestamp">A new timestamp to assign to the updated task records.</param>
    /// <returns>An array containing the updated tasks.</returns>
    /// <remarks>
    /// Depending on the number of available records in the database the returned collection
    /// of tasks might contain fewer than <paramref name="count"/> task records.
    /// </remarks>
    public DatabaseTaskDto[] UpdateAndGetTasks(
        int count,
        StringId taskPoolId,
        short stateId,
        short updatedStateId,
        DateTime upperTimestampLimit,
        DateTime updatedTimestamp)
    {
      Assert.ArgumentCondition(count > 0, "count", "The number of tasks to retrieve must be positive.");
      Assert.ArgumentNotNull(taskPoolId, "taskPoolId");
      Assert.ArgumentCondition(updatedTimestamp > upperTimestampLimit, "updatedTimestamp", "The updated timestamp must be newer than the upper timestamp limit.");

      return SqlUtil.InvokeWithDeadlockRetry(
          () =>
          {
            var tasks = new List<DatabaseTaskDto>(count);

            using (var connection = new SqlConnection(_connectionString))
            {
              connection.Open();
              using (var command = connection.CreateCommand())
              {
                command.CommandText = string.Format(
                          "UPDATE TOP (@Count) [{0}] WITH (TABLOCK) "
                              + "SET [StateID]=@UpdatedStateId,[Timestamp]=@UpdatedTimestamp,[Generation]=[Generation]+1 "
                              + "OUTPUT INSERTED.[TaskID],INSERTED.[Generation],INSERTED.[TaskData] "
                              + "WHERE [TaskPoolID]=@TaskPoolID AND [StateID]=@StateId AND [Timestamp]<@UpperTimestampLimit",
                          _tableName);

                command.CommandTimeout = commandTimeout;
                command.Parameters.Add(new SqlParameter("@UpdatedStateId", updatedStateId));
                command.Parameters.Add(new SqlParameter("@UpdatedTimestamp", updatedTimestamp));
                command.Parameters.Add(new SqlParameter("@Count", count));
                command.Parameters.Add(new SqlParameter("@TaskPoolID", taskPoolId.Value));
                command.Parameters.Add(new SqlParameter("@StateId", stateId));
                command.Parameters.Add(new SqlParameter("@UpperTimestampLimit", upperTimestampLimit));

                using (var dataReader = command.ExecuteReader())
                {
                  while (dataReader.Read())
                  {
                    var taskId = dataReader.GetSqlBinary(0);
                    var generation = dataReader.GetInt32(1);
                    var taskData = dataReader.GetString(2);

                    var task = new DatabaseTaskDto(taskId.Value, taskPoolId, updatedStateId, updatedTimestamp, generation, taskData);
                    tasks.Add(task);
                  }
                }
              }
            }

            return tasks.ToArray();
          },
          NumDeadlockAttempts);
    }

    /// <summary>
    /// Updates a collection of task records in the database.
    /// </summary>
    /// <param name="tasks">The collection of tasks containing updated values.</param>
    public void UpdateTasks(IEnumerable<DatabaseTaskDto> tasks)
    {
      Assert.ArgumentNotNull(tasks, "tasks");

      // Convert the tasks to a data table.
      var tasksTable = GetTasksDtoTable();

      foreach (var task in tasks)
      {
        tasksTable.Rows.Add(task.TaskId.Value, string.Empty, task.StateId, task.Timestamp, task.TaskData);
      }

      // Update the tasks.
      SqlUtil.InvokeWithDeadlockRetry(
          () =>
          {
            using (var connection = new SqlConnection(_connectionString))
            {
              connection.Open();
              using (var command = connection.CreateCommand())
              {
                command.CommandText = string.Format(
                          "UPDATE OT "
                              + "SET OT.[StateID]=IT.[StateID],OT.[Timestamp]=IT.[Timestamp],[Generation]=[Generation]+1,OT.[TaskData]=IT.[TaskData] "
                              + "FROM @Tasks IT "
                              + "INNER JOIN [{0}] OT ON OT.[TaskID]=IT.[TaskID]",
                          _tableName);

                command.CommandTimeout = commandTimeout;
                var sessionIdsParam = command.Parameters.AddWithValue("@Tasks", tasksTable);
                sessionIdsParam.SqlDbType = SqlDbType.Structured;
                sessionIdsParam.TypeName = tasksTable.TableName;

                command.ExecuteNonQuery();
              }
            }
          },
          NumDeadlockAttempts);
    }

    /// <summary>
    /// Updates the timestamp on a collection of task records in the database.
    /// </summary>
    /// <param name="taskIds">The set of ids of tasks to update.</param>
    /// <param name="updatedTimestamp">A new timestamp to assign to the task records.</param>
    public void UpdateTaskTimestamps(IEnumerable<BinaryId48> taskIds, DateTime updatedTimestamp)
    {
      Assert.ArgumentNotNull(taskIds, "taskIds");

      // Convert the task ids to a data table.
      var taskIdsTable = GetTasksIdsTable();

      foreach (var taskId in taskIds)
      {
        taskIdsTable.Rows.Add(taskId.Value);
      }

      // Remove the tasks.
      SqlUtil.InvokeWithDeadlockRetry(
          () =>
          {
            using (var connection = new SqlConnection(_connectionString))
            {
              connection.Open();
              using (var command = connection.CreateCommand())
              {
                command.CommandText = string.Format("UPDATE [{0}] SET [Timestamp]=@UpdatedTimestamp WHERE [TaskID] IN (SELECT [TaskID] FROM @TaskIDs)", _tableName);

                command.CommandTimeout = commandTimeout;
                command.Parameters.Add(new SqlParameter("@UpdatedTimestamp", updatedTimestamp));
                var sessionIdsParam = command.Parameters.AddWithValue("@TaskIDs", taskIdsTable);
                sessionIdsParam.SqlDbType = SqlDbType.Structured;
                sessionIdsParam.TypeName = taskIdsTable.TableName;

                command.ExecuteNonQuery();
              }
            }
          },
          NumDeadlockAttempts);
    }

    /// <summary>
    /// Gets a table with columns matching the user-defined table dbo.TasksDto.
    /// </summary>
    /// <returns>A new <see cref="DataTable"/>.</returns>
    [NotNull]
    private static DataTable GetTasksIdsTable()
    {
      var result = new DataTable("dbo.TasksIDs");

      result.Columns.Add("TaskID", typeof(byte[]));

      return result;
    }

    /// <summary>
    /// Gets a table with columns matching the user-defined table dbo.TasksDto.
    /// </summary>
    /// <returns>A new <see cref="DataTable"/>.</returns>
    [NotNull]
    private static DataTable GetTasksDtoTable()
    {
      var result = new DataTable("dbo.TasksDto");

      result.Columns.Add("TaskID", typeof(byte[]));
      result.Columns.Add("TaskPoolID", typeof(string));
      result.Columns.Add("StateID", typeof(short));
      result.Columns.Add("Timestamp", typeof(DateTime));
      result.Columns.Add("TaskData", typeof(string));

      return result;
    }
  }
}
