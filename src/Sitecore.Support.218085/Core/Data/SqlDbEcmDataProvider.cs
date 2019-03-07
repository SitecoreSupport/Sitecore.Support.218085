// © 2016 Sitecore Corporation A/S. All rights reserved.

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Newtonsoft.Json;
using Sitecore.Diagnostics;
using Sitecore.EmailCampaign.Analytics.Model;
using Sitecore.ExM.Framework.Diagnostics;
using Sitecore.Modules.EmailCampaign;
using Sitecore.Modules.EmailCampaign.Core.Dispatch;
using Sitecore.Modules.EmailCampaign.Core.Gateways;
using Sitecore.Modules.EmailCampaign.Core.Gateways.Models;
using Sitecore.Modules.EmailCampaign.Messages;

namespace Sitecore.Support.Modules.EmailCampaign.Core.Data
{
  /// <summary>
  ///   The SQL Server data provider for EXM.
  ///   Used for:
  ///   - Persisting campaign message statistics
  ///   - Managing recipients for the dispatch pipeline.
  /// </summary>
  public class SqlDbEcmDataProvider : Sitecore.Modules.EmailCampaign.Core.Data.SqlDbEcmDataProvider
  {

    private TimeSpan _commandTimeout;
    /// <summary>
    /// Initialize provider.
    /// </summary>
    /// <param name="name">The name.</param>
    /// <param name="config">The config.</param>
    public override void Initialize(string name, NameValueCollection config)
    {
      Assert.ArgumentNotNull(name, "name");
      Assert.ArgumentNotNull(config, "config");

      base.Initialize(name, config);
      _connectionStringName = config.Get("connectionStringName");
      _commandTimeout = DateUtil.ParseTimeSpan(config.Get("commandTimeout"), TimeSpan.FromMinutes(5.0));
    }

    internal List<string> GetUpdateCampaignCommandDateFields(bool hasStartDate, bool hasEndDate)
    {
      var updateFields = new List<string>();

      if (hasStartDate)
      {
        updateFields.Add("[StartDate] = @StartDate");
      }

      if (hasEndDate)
      {
        updateFields.Add("[EndDate] = @EndDate");
      }

      return updateFields;
    }

    internal string GetUpdateCampaignCommandTextRecipientField(string fieldName, FieldUpdate<int> field)
    {
      var hasRecipientCount = field != null;
      var incrementRecipients = hasRecipientCount && field.Option == UpdateOption.Add;

      if (hasRecipientCount && !incrementRecipients)
      {
        return string.Format("[{0}] = @{0}", fieldName);
      }

      if (hasRecipientCount)
      {
        return string.Format("[{0}] += @{0}", fieldName);
      }

      return null;
    }

    /// <summary>
    /// Get the DispatchQueueItem returned from the database
    /// </summary>
    /// <param name="dataReader"> The data Reader. </param>
    /// <returns> The <see cref="DispatchQueueItem"/>. </returns>
    private static DispatchQueueItem ProcessDispatchQueueItem(SqlDataReader dataReader)
    {
      return new DispatchQueueItem
      {
        Id = dataReader.GetGuid(0),
        MessageId = dataReader.GetGuid(1),
        RecipientId = dataReader.GetString(2),
        ContactId = dataReader.GetGuid(3),
        RecipientQueue = (RecipientQueue)dataReader.GetByte(4),
        DispatchType = (DispatchType)dataReader.GetByte(5),
        LastModified = dataReader.GetDateTime(6),
        MessageType = (MessageType)dataReader.GetByte(7),
        CustomPersonTokens = JsonConvert.DeserializeObject<Dictionary<string, object>>(dataReader.IsDBNull(8) ? string.Empty : dataReader.GetString(8)),
        InProgress = dataReader.GetBoolean(9)
      };
    }

    #region Private Properties

    /// <summary>
    /// The connection string
    /// </summary>
    private string _connectionString;

    /// <summary>
    ///   The name of the connection string
    /// </summary>
    private string _connectionStringName;

    /// <summary>
    /// Gets the connection string.
    /// </summary>
    public string ConnectionString
    {
      get
      {
        if (string.IsNullOrWhiteSpace(_connectionStringName) && string.IsNullOrWhiteSpace(_connectionString))
        {
          return null;
        }

        if (!string.IsNullOrWhiteSpace(_connectionString))
        {
          return _connectionString;
        }

        _connectionString = ConfigurationManager.ConnectionStrings[_connectionStringName].ConnectionString;
        return _connectionString;
      }

      internal set { _connectionString = value; }
    }

    /// <summary>
    /// Gets or sets the logger.
    /// </summary>
    public ILogger Logger { get; set; }

    /// <summary>
    ///   Gets a value indicating whether the EcmDataProvider is available and in the healthy state.
    ///   For example, if the provider using a database to work with data, and database is down, property should return
    ///   <c>false</c>
    /// </summary>
    public override bool ProviderAvailable
    {
      get
      {
        try
        {
          using (var connection = new SqlConnection(ConnectionString))
          {
            connection.Open();
            return true;
          }
        }
        catch (Exception)
        {
          return false;
        }
      }
    }

    #endregion;

    #region Message statistic

    /// <summary>
    /// Creates a new campaign record for an e-mail message.
    /// </summary>
    /// <param name="campaignId">Id of campaign.</param>
    /// <param name="messageId">Id of the email message.</param>
    /// <param name="rootManagerId">Id of the root manager item related to the email message.</param>
    /// <param name="startDate">Start date provided as local time.</param>
    /// <param name="endDate">End date provided as local time.</param>
    public override void CreateCampaign(Guid campaignId, Guid messageId, Guid rootManagerId, DateTime startDate, DateTime endDate)
    {
      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var command = connection.CreateCommand())
        {
          command.CommandText = "INSERT INTO [Campaigns] ([ID],[MessageID],[ManagerRootID],[StartDate],[EndDate]) " +
              "VALUES(@ID, @MessageID, @ManagerRootID, @StartDate, @EndDate)";
          command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;

          command.Parameters.Add(new SqlParameter("@ID", campaignId));
          command.Parameters.Add(new SqlParameter("@MessageID", messageId));
          command.Parameters.Add(new SqlParameter("@ManagerRootID", rootManagerId));
          command.Parameters.Add(new SqlParameter("@StartDate", startDate.ToUniversalTime()));
          command.Parameters.Add(new SqlParameter("@EndDate", endDate.ToUniversalTime()));
          command.ExecuteNonQuery();
        }
      }
    }

    /// <summary>
    /// Retrieve a given campaign record.
    /// </summary>
    /// <param name="campaignId"> Id of the campaign record to retrieve. </param>
    /// <returns> The specified campaign record. <see langword="null"/> is returned if no campaign record with the specified id   was found. </returns>
    public override EmailCampaignsData GetCampaign(Guid campaignId)
    {
      EmailCampaignsData campaign = null;

      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var command = connection.CreateCommand())
        {
          command.CommandText = "SELECT [ID],[MessageID],[ManagerRootID],[StartDate],[EndDate],[TotalRecipients],[IncludedRecipients],[ExcludedRecipients],[GloballyExcluded] FROM [Campaigns] WHERE [ID] = @ID";

          command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
          command.Parameters.Add(new SqlParameter("@ID", campaignId));

          using (var reader = command.ExecuteReader(CommandBehavior.SingleRow))
          {
            if (reader.Read())
            {
              campaign = new EmailCampaignsData
              {
                Id = reader.GetGuid(0),
                MessageId = reader.GetGuid(1),
                ManagerRootId = reader.GetGuid(2),
                StartDate = reader.GetDateTime(3),
                EndDate = reader.GetDateTime(4),
                TotalRecipients = reader.GetInt32(5),
                IncludedRecipients = reader.GetInt32(6),
                ExcludedRecipients = reader.GetInt32(7),
                GloballyExcluded = reader.GetInt32(8)
              };
            }
          }
        }
      }

      if (campaign == null)
      {
        return null;
      }

      campaign.StartDate = Util.ConvertRealDateToLocalTime(campaign.StartDate);
      campaign.EndDate = Util.ConvertRealDateToLocalTime(campaign.EndDate);

      return campaign;
    }

    /// <summary>
    /// Sets the end date of a given campaign.
    /// </summary>
    /// <param name="campaignId">The campaign id.</param>
    /// <param name="endDate">New end date provided as local time.</param>
    public override void SetCampaignEndDate(Guid campaignId, DateTime endDate)
    {
      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var command = connection.CreateCommand())
        {
          command.CommandText = "UPDATE [Campaigns] SET [EndDate] = @EndDate WHERE [ID] = @ID";

          command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
          command.Parameters.Add(new SqlParameter("@EndDate", endDate.ToUniversalTime()));
          command.Parameters.Add(new SqlParameter("@ID", campaignId));
          command.ExecuteNonQuery();
        }
      }
    }

    /// <summary>
    /// Sets the start and end dates of a given campaign.
    /// </summary>
    /// <param name="campaignId"> The campaign id. </param>
    /// <param name="startDate"> The start Date. </param>
    /// <param name="endDate"> The end Date. </param>
    /// <param name="recipientsCount"> Count of recipients </param>
    /// <param name="includedRecipients">Number of included recipients</param>
    /// <param name="excludedRecipients">Number of excluded recipients</param>
    /// <param name="globallyExcluded">Number of globally excluded recipients</param>/// 
    public override void SetMessageStatisticData(Guid campaignId, DateTime? startDate, DateTime? endDate, [CanBeNull] FieldUpdate<int> recipientsCount, FieldUpdate<int> includedRecipients, FieldUpdate<int> excludedRecipients, FieldUpdate<int> globallyExcluded)
    {
      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var command = connection.CreateCommand())
        {
          var updateFields = GetUpdateCampaignCommandDateFields(startDate.HasValue, endDate.HasValue);

          var recipientFields = new List<Tuple<string, FieldUpdate<int>>>
                    {
                        new Tuple<string, FieldUpdate<int>>("TotalRecipients", recipientsCount),
                        new Tuple<string, FieldUpdate<int>>("IncludedRecipients", includedRecipients),
                        new Tuple<string, FieldUpdate<int>>("ExcludedRecipients", excludedRecipients),
                        new Tuple<string, FieldUpdate<int>>("GloballyExcluded", globallyExcluded),
                    };

          updateFields.AddRange(recipientFields.Select(recipientField => GetUpdateCampaignCommandTextRecipientField(recipientField.Item1, recipientField.Item2)).Where(field => field != null));

          command.CommandText = string.Format("UPDATE [Campaigns] SET {0} WHERE [ID] = @ID", string.Join(", ", updateFields));

          command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
          if (startDate.HasValue)
          {
            var startDateSqlParameter = new SqlParameter("@StartDate", startDate.Value.ToUniversalTime());

            if (endDate.HasValue)
            {
              command.Parameters.Add(startDateSqlParameter);
              command.Parameters.Add(new SqlParameter("@EndDate", endDate.Value.ToUniversalTime()));
            }
            else
            {
              command.Parameters.Add(startDateSqlParameter);
            }
          }
          else if (endDate.HasValue)
          {
            command.Parameters.Add(new SqlParameter("@EndDate", endDate.Value.ToUniversalTime()));
          }

          foreach (var recipientField in recipientFields)
          {
            if (recipientField.Item2 != null)
            {
              command.Parameters.Add(new SqlParameter("@" + recipientField.Item1, recipientField.Item2.Value));
            }
          }

          command.Parameters.Add(new SqlParameter("@ID", campaignId));
          command.ExecuteNonQuery();
        }
      }
    }

    #endregion;

    #region Dispatch Queue

    /// <summary>
    /// Adds a set of recipients to the dispatch queue for a given message.
    /// </summary>
    /// <param name="messageId">Id of the message.</param>
    /// <param name="messageType">Message Type of the message.</param>
    /// <param name="recipients">A set of recipients described by their recipient id, contact id, and type in relation to A/B testing.</param>
    /// <param name="customPersonTokens">List of custom person tokens.</param>
    public override void AddToDispatchQueue(Guid messageId, MessageType messageType, [NotNull] IEnumerable<Tuple<string, Guid, DispatchType>> recipients, Dictionary<string, object> customPersonTokens = null)
    {
      // Input Validation
      Assert.ArgumentNotNull(recipients, "recipients");

      // Add to queue.
      var queueItems = recipients.Select(
          q => new DispatchQueueItem
          {
            MessageId = messageId,
            RecipientId = q.Item1,
            ContactId = q.Item2,
            RecipientQueue = q.Item3 == DispatchType.AbTest ? RecipientQueue.AbTestRecipient : RecipientQueue.Recipient,
            DispatchType = q.Item3,
            LastModified = DateTime.UtcNow,
            MessageType = messageType,
            CustomPersonTokens = customPersonTokens
          });

      // Store dispatch queue item id
      if (EngagementAnalyticsPlanSendingContextSwitcher.IsActive)
      {
        var queueItemsArr = queueItems.ToArray();
        queueItems = queueItemsArr;
        if (queueItemsArr.Length == 1)
        {
          EngagementAnalyticsPlanSendingContextSwitcher.CurrentValue.ExactDispatchQueueElemId = queueItemsArr[0].Id;
        }
      }

      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var sqlBulkCopy = new SqlBulkCopy(connection))
        {
          sqlBulkCopy.DestinationTableName = "DispatchQueue";
          using (var dispatchQueueItemDataReader = new DispatchQueueItemDataReader(queueItems, Logger))
          {
            sqlBulkCopy.WriteToServer(dispatchQueueItemDataReader);
          }
        }
      }
    }

    /// <summary>
    /// Removes all recipients for <paramref name="messageId"/> from the dispatch queue.
    /// </summary>
    /// <param name="messageId">Id of the message.</param>
    public override void ClearDispatchQueue(Guid messageId)
    {
      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var command = connection.CreateCommand())
        {
          command.CommandText = "DELETE FROM [DispatchQueue] WHERE [MessageID] = @MessageID";

          command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
          command.Parameters.Add(new SqlParameter("@MessageID", messageId));
          command.ExecuteNonQuery();
        }
      }
    }

    /// <summary>
    /// Counts the number of recipients for <paramref name="messageId"/> in a given dispatch queue. An optional filter on queue states can be applied.
    /// </summary>
    /// <param name="messageId">Id of the message.</param>
    /// <param name="queueStates">Optional collection of queue states to filter by. If no queue state is specified the total amount will be returned.</param>
    /// <returns>The amount of recipients.</returns>
    public override long CountRecipientsInDispatchQueue(Guid messageId, params RecipientQueue[] queueStates)
    {
      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var command = connection.CreateCommand())
        {
          command.CommandText = "SELECT COUNT([ID]) FROM [DispatchQueue] WHERE [MessageId] = @MessageId";
          command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
          if (queueStates != null && queueStates.Length > 0)
          {
            command.CommandText += string.Format(" AND [RecipientQueue] IN ({0})", string.Join(",", queueStates.Cast<int>().ToArray()));
          }

          command.Parameters.Add(new SqlParameter("@MessageId", messageId));

          return System.Convert.ToInt64(command.ExecuteScalar());
        }
      }
    }

    /// <summary>
    /// Delete a specific recipient from its dispatch queue.
    /// </summary>
    /// <param name="queueItemId">Id of item used publicly in the dispatch queue.</param>
    public override void DeleteRecipientsFromDispatchQueue(Guid queueItemId)
    {
      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var command = connection.CreateCommand())
        {
          command.CommandText = "DELETE FROM [DispatchQueue] WHERE [ID] = @ID";

          command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
          command.Parameters.Add(new SqlParameter("@ID", queueItemId));
          command.ExecuteNonQuery();
        }
      }
    }

    /// <summary>
    /// Get all messages, for a specific message type, where state is in progress
    /// </summary>
    /// <param name="messageType">
    /// The message type. 
    /// </param>
    /// <param name="timeout">
    /// The time Span.
    /// </param>
    /// <returns>
    /// The <see cref="IEnumerable{T}"/>.
    /// </returns>
    public override IEnumerable<Guid> GetMessagesInProgress(MessageType messageType, TimeSpan timeout)
    {
      var messageIds = new List<Guid>();
      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var command = connection.CreateCommand())
        {
          command.CommandText = "SELECT DISTINCT([MessageID]) FROM [DispatchQueue] WHERE [MessageType] = @MessageType AND [InProgress] = 1 AND [LastModified] < @LastModified";

          command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
          command.Parameters.Add(new SqlParameter("@MessageType", messageType));
          command.Parameters.Add(new SqlParameter("@LastModified", DateTime.UtcNow.Subtract(timeout)));

          using (var dataReader = command.ExecuteReader())
          {
            while (dataReader.Read())
            {
              messageIds.Add(dataReader.GetGuid(0));
            }
          }
        }
      }

      return messageIds;
    }

    /// <summary>
    /// Retrieves a recipient that is ready for dispatch and moves the recipient to the state in progress.
    /// </summary>
    /// <param name="messageId">Id of the message.</param>
    /// <param name="queueState">The queue to get the recipient from</param>
    /// <returns>A recipient that is ready ready for dispatch or <see langword="null"/> if no recipients were found.</returns>
    public override DispatchQueueItem GetNextRecipientForDispatch(Guid messageId, RecipientQueue queueState)
    {
      DispatchQueueItem dispatchQueueItem;

      var currentEngagementAnalyticsPlanSendingContext = EngagementAnalyticsPlanSendingContextSwitcher.CurrentValue;

      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();

        using (var command = connection.CreateCommand())
        {
          command.CommandText = "UPDATE TOP(1) [DispatchQueue] SET [InProgress] = 1, [LastModified] = @LastModified " +
              "OUTPUT inserted.ID, inserted.MessageID, inserted.RecipientID, inserted.ContactID, inserted.RecipientQueue, inserted.DispatchType, " +
              "inserted.LastModified, inserted.MessageType, inserted.CustomPersonTokens, inserted.InProgress " +
              "WHERE [InProgress] = 0";

          command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
          command.Parameters.Add(new SqlParameter("@LastModified", DateTime.UtcNow));

          if (currentEngagementAnalyticsPlanSendingContext == null)
          {
            command.CommandText += " AND [MessageID] = @MessageID";
            command.Parameters.Add(new SqlParameter("@MessageID", messageId));

            command.CommandText += " AND [RecipientQueue] = @RecipientQueue";
            command.Parameters.Add(new SqlParameter("@RecipientQueue", queueState));
          }
          else
          {
            command.CommandText += " AND [ID] = @ID";
            command.Parameters.Add(new SqlParameter("@ID", currentEngagementAnalyticsPlanSendingContext.ExactDispatchQueueElemId));
          }

          using (var dataReader = command.ExecuteReader(CommandBehavior.SingleRow))
          {
            if (!dataReader.Read())
            {
              return null;
            }

            dispatchQueueItem = ProcessDispatchQueueItem(dataReader);
          }
        }
      }

      return dispatchQueueItem;
    }

    /// <summary>
    /// Resets the process flag of all recipients in a dispatch queue.
    /// </summary>
    /// <param name="messageId">Id of the message.</param>
    /// <param name="timeout">Timeout to filter date with.</param>
    public override void ResetProcessState(Guid messageId, TimeSpan? timeout = null)
    {
      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var command = connection.CreateCommand())
        {
          command.CommandText = "UPDATE [DispatchQueue] SET [InProgress] = 0, [LastModified] = @LastModified " +
              "WHERE [MessageID] = @MessageID AND [InProgress] = 1";

          command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
          command.Parameters.Add(new SqlParameter("@LastModified", DateTime.UtcNow));
          command.Parameters.Add(new SqlParameter("@MessageID", messageId));

          if (timeout == default(TimeSpan))
          {
            command.CommandText += " AND [LastModified] < @Timeout";
            command.Parameters.Add(new SqlParameter("@Timeout", DateTime.UtcNow.Subtract(timeout.Value)));
          }

          command.ExecuteNonQuery();
        }
      }
    }

    /// <summary>
    /// Changes the dispatch type of a number of items in the dispatch queue of a given message.
    /// </summary>
    /// <param name="messageId">The id of the message.</param>
    /// <param name="sourceType">The dispatch type of the items to change.</param>
    /// <param name="targetType">The dispatch type the items should be changed to.</param>
    /// <param name="itemCount">The number of items to change.</param>
    public override void ChangeDispatchType(Guid messageId, DispatchType sourceType, DispatchType targetType, int itemCount)
    {
      var sourceQueue = (sourceType == DispatchType.AbTest) ? RecipientQueue.AbTestRecipient : RecipientQueue.Recipient;
      var targetQueue = (targetType == DispatchType.AbTest) ? RecipientQueue.AbTestRecipient : RecipientQueue.Recipient;

      using (var connection = new SqlConnection(ConnectionString))
      {
        connection.Open();
        using (var command = connection.CreateCommand())
        {
          command.CommandText = "UPDATE TOP(@ItemCount) [DispatchQueue] " +
              "SET [RecipientQueue] = @TargetQueue, [DispatchType] = @TargetType, [LastModified] = @LastModified " +
              "WHERE [MessageID] = @MessageID AND [RecipientQueue] = @SourceQueue AND [InProgress] = 0";

          command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
          command.Parameters.Add(new SqlParameter("@ItemCount", itemCount));
          command.Parameters.Add(new SqlParameter("@TargetQueue", targetQueue));
          command.Parameters.Add(new SqlParameter("@TargetType", targetType));
          command.Parameters.Add(new SqlParameter("@LastModified", DateTime.UtcNow));
          command.Parameters.Add(new SqlParameter("@MessageID", messageId));
          command.Parameters.Add(new SqlParameter("@SourceQueue", sourceQueue));

          command.ExecuteNonQuery();
        }
      }
    }

    #endregion
  }
}
