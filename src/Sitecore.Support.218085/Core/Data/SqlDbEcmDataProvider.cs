// © 2016 Sitecore Corporation A/S. All rights reserved.

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using Newtonsoft.Json;
using Sitecore.Analytics.Data.Items;
using Sitecore.Data.Items;
using Sitecore.Diagnostics;
using Sitecore.EmailCampaign.Model.Data;
using Sitecore.EmailCampaign.Model.Dispatch;
using Sitecore.EmailCampaign.Model.Message;
using Sitecore.ExM.Framework.Diagnostics;
using Sitecore.Globalization;
using Sitecore.Modules.EmailCampaign.Core.Dispatch;
using Sitecore.Modules.EmailCampaign.Core.Gateways;
using Sitecore.Modules.EmailCampaign.Messages;
using Sitecore.Modules.EmailCampaign.Core.Data;
using Sitecore.Modules.EmailCampaign.Core;
using System.Globalization;

namespace Sitecore.Support.Modules.EmailCampaign.Core.Data
{
    /// <summary>
    ///   The SQL Server data provider for EXM.
    ///   Used for:
    ///   - Persisting campaign message statistics
    ///   - Managing recipients for the dispatch pipeline.
    /// </summary>
    public class SqlDbEcmDataProvider : EcmDataProvider
    {
        private readonly IMessageStateInfoFactory _messageStateInfoFactory;
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

        /// <summary>
        /// Initializes static members
        /// </summary>
        static SqlDbEcmDataProvider()
        {
            AddSortFieldMappings();
        }

        public SqlDbEcmDataProvider() : this(new MessageStateInfoFactory())
        {
        }

        public SqlDbEcmDataProvider([NotNull] IMessageStateInfoFactory messageStateInfoFactory)
        {
            Assert.ArgumentNotNull(messageStateInfoFactory, nameof(messageStateInfoFactory));

            _messageStateInfoFactory = messageStateInfoFactory;
        }

        #region Private Properties

        /// <summary>
        /// Campaign fields that should be updated by the message statistics.
        /// FailedRecipients and SkippedRecipients are not included, as those 
        /// values are updated separately by each participating dispatch server. 
        /// If those fields were added to this list, there's a chance the message 
        /// statistics will be retrieved before an update of the 
        /// FailedRecipients/SkippedRecipients, and when the MessageStatistics are 
        /// saved, the FailedRecipients/SkippedRecipients count would be off.
        /// </summary>
        private static readonly string[] CampaignFields =
        {
            "ManagerRootID",
            "CampaignID",
            "StartDate",
            "EndDate",
            "TotalRecipients",
            "IncludedRecipients",
            "ExcludedRecipients",
            "GloballyExcluded",
            "SentRecipients",
            "UnprocessedRecipients",
            "BouncedRecipients",
            "MessageName",
            "Subject",
            "MessageType",
            "Status",
            "CreatedDate",
            "ModifiedDate",
            "StatusDate",
            "Opens",
            "OpenRate",
            "UniqueOpens",
            "Clicks",
            "UniqueClicks",
            "ClickRate",
            "Value",
            "ValuePerEmail",
            "ValuePerVisit",
            "PageBounces",
            "Unsubscribes",
            "Spams",
            "Browsed",
            "ScheduledDate",
            "AbTest"
        };

        private string _connectionStringName;
        private string _connectionString;
        private static Dictionary<string, string> _sortFieldMapping;
        private const int DefaultPageSize = 25;

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
                    using (var connection = new SqlConnection(_connectionString))
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

        #endregion;

        #region Message statistic

        /// <inheritdoc />
        public override EmailCampaignsData GetCampaign(Guid messageId)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT * FROM Campaigns WHERE MessageID = @MessageID";
                    command.Parameters.Add(new SqlParameter("@MessageID", messageId));
                    command.CommandTimeout = (int) _commandTimeout.TotalMilliseconds;

                    using (SqlDataReader reader = command.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        if (reader.Read())
                        {
                            return ToDomain(reader);
                        }

                        return null;
                    }
                }
            }
        }

        /// <inheritdoc />
        public override void SetCampaignEndDate(Guid messageId, DateTime endDate)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "UPDATE [Campaigns] SET [EndDate] = @EndDate WHERE MessageID = @ID";
                    command.Parameters.Add(new SqlParameter("@EndDate", endDate.ToUniversalTime()));
                    command.Parameters.Add(new SqlParameter("@ID", messageId));

                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <inheritdoc />
        public override void SetSubject(Guid messageId, string subject)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "UPDATE [Campaigns] SET [Subject] = @Subject WHERE MessageID = @ID";
                    command.Parameters.Add(new SqlParameter("@Subject", subject));
                    command.Parameters.Add(new SqlParameter("@ID", messageId));
                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <inheritdoc />
        public override void SetMessageStatisticData(Guid messageId, DateTime? startDate, DateTime? endDate, [CanBeNull] FieldUpdate<int> recipientsCount, FieldUpdate<int> includedRecipients, FieldUpdate<int> excludedRecipients, FieldUpdate<int> globallyExcluded, FieldUpdate<int> skippedRecipients, FieldUpdate<int> failedRecipients)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    var updateFields = GetUpdateCampaignCommandDateFields(startDate.HasValue, endDate.HasValue);

                    updateFields.Add("[ModifiedDate] = @ModifiedDate");

                    var recipientFields = new List<Tuple<string, FieldUpdate<int>>>
                    {
                        new Tuple<string, FieldUpdate<int>>("TotalRecipients", recipientsCount),
                        new Tuple<string, FieldUpdate<int>>("IncludedRecipients", includedRecipients),
                        new Tuple<string, FieldUpdate<int>>("ExcludedRecipients", excludedRecipients),
                        new Tuple<string, FieldUpdate<int>>("GloballyExcluded", globallyExcluded),
                        new Tuple<string, FieldUpdate<int>>("SkippedRecipients", skippedRecipients),
                        new Tuple<string, FieldUpdate<int>>("FailedRecipients", failedRecipients)
                    };

                    updateFields.AddRange(recipientFields.Select(recipientField => GetUpdateCampaignCommandTextRecipientField(recipientField.Item1, recipientField.Item2)).Where(field => field != null));

                    command.CommandText = string.Format("UPDATE [Campaigns] SET {0} WHERE [MessageID] = @ID", string.Join(", ", updateFields));

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

                    command.Parameters.Add(new SqlParameter("@ModifiedDate", DateTime.UtcNow));
                    command.Parameters.Add(new SqlParameter("@ID", messageId));
                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;

                    command.ExecuteNonQuery();
                }
            }
        }

        /// <inheritdoc />
        public override void CreateCampaign(EmailCampaignsData data)
        {
            Assert.ArgumentNotNull(data, nameof(data));

            data.ModifiedDate = DateTime.UtcNow;

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = GetCampaignInsertCommandText();
                    AddParameters(data, command);
                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <inheritdoc />
        public override void SaveCampaign(EmailCampaignsData data)
        {
            Assert.ArgumentNotNull(data, nameof(data));

            data.ModifiedDate = DateTime.UtcNow;

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = GetCampaignUpdateCommandText();

                    AddParameters(data, command);
                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;

                    command.ExecuteNonQuery();
                }
            }
        }

        /// <inheritdoc />
        public override EmailCampaignsData SaveCampaign(MessageItem message, Item campaignItem, SaveCampaignFlags flags = SaveCampaignFlags.None, EmailCampaignsData data = null)
        {
            Assert.ArgumentNotNull(message, nameof(message));

            if (data == null)
            {
                data = GetCampaign(message.MessageId);
            }

            var isNew = false;
            if (data == null)
            {
                isNew = true;

                data = new EmailCampaignsData
                {
                    MessageId = message.MessageId,
                    CreatedDate = message.InnerItem.Created,
                    ManagerRootId = message.ManagerRoot.InnerItem.ID.Guid
                };
            }

            data.MessageName = message.Name;
            data.MessageType = message.MessageType;

            if (data.Subject == null && !(message is WebPageMail))
            {
                data.Subject = message.Subject;
            }

            if (!data.CampaignId.HasValue && campaignItem != null)
            {
                data.CampaignId = campaignItem.ID.Guid;
                data.StartDate = DateUtil.ParseDateTime(campaignItem[CampaignItem.FieldIDs.StartDate], DateTime.UtcNow);
                data.EndDate = DateUtil.ParseDateTime(campaignItem[CampaignItem.FieldIDs.EndDate], DateTime.MaxValue);
            }

            using (new LanguageSwitcher("en"))
            {
                var info = _messageStateInfoFactory.CreateInstance(message);

                data.TestSizePercent = info.TestSizePercent;
                data.AbTest = info.HasAbn;
                data.Status = info.Status;
                data.StatusDate = info.StateDate;
                data.ScheduledDate = info.Scheduled;

                if ((flags & SaveCampaignFlags.UpdateQueueing) == SaveCampaignFlags.UpdateQueueing)
                {
                    data.UnprocessedRecipients = info.Queuing;
                }
            }

            if (isNew)
            {
                CreateCampaign(data);
            }
            else
            {
                SaveCampaign(data);
            }

            return data;
        }

        /// <inheritdoc />
        public override CampaignSearchResults SearchCampaigns(CampaignSearchOptions options)
        {
            Assert.ArgumentNotNull(options, nameof(options));

            if (options.Page == null)
            {
                options.Page = new DataPage { Size = DefaultPageSize };
            }

            var results = new CampaignSearchResults
            {
                Results = new List<EmailCampaignsData>()
            };

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                var innerCommandText = "Campaigns";
                var filters = new List<string>();
                if (options.ManagerRootId != Guid.Empty)
                {
                    filters.Add($"ManagerRootID='{options.ManagerRootId}'");
                }

                if (options.SentDateFrom.HasValue)
                {
                    filters.Add("StatusDate >= @StatusDateFrom");
                }

                if (options.SentDateTo.HasValue)
                {
                    filters.Add("StatusDate <= @StatusDateTo");
                }

                if (!string.IsNullOrEmpty(options.SearchExpression))
                {
                    filters.Add("(MessageName LIKE @SearchExpression OR Subject LIKE @SearchExpression)");
                }

                if (options.StatusInclude != null && options.StatusInclude.Any())
                {
                    var includeParameters = new List<string>();
                    for (var i = 0; i < options.StatusInclude.Length; i++)
                    {
                        includeParameters.Add("@includeStatus" + i);
                    }

                    string status = string.Join(", ", includeParameters);
                    filters.Add($"Status IN ({status})");
                }

                if (options.StatusExclude != null && options.StatusExclude.Any())
                {
                    var excludeParameters = new List<string>();
                    for (var i = 0; i < options.StatusExclude.Length; i++)
                    {
                        excludeParameters.Add("@excludeStatus" + i);
                    }

                    string status = string.Join(", ", excludeParameters);
                    filters.Add($"Status NOT IN ({status})");
                }

                if (filters.Any())
                {
                    innerCommandText = $"{innerCommandText} WHERE {string.Join(" AND ", filters)}";
                }

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT COUNT(MessageID) FROM " + innerCommandText;
                    Logger.LogDebug(command.CommandText);

                    AddSearchParameters(options, command);


                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
                    results.TotalResults = (int)command.ExecuteScalar();
                }

                if (results.TotalResults == 0)
                {
                    return results;
                }

                using (SqlCommand command = connection.CreateCommand())
                {
                    int rowFirst = options.Page.Index * options.Page.Size + 1;
                    int rowLast = rowFirst + options.Page.Size - 1;

                    var sortParameter = "(SELECT NULL)";
                    if (options.SortParameter?.Name != null)
                    {
                        if (!_sortFieldMapping.ContainsKey(options.SortParameter.Name))
                        {
                            Logger.LogWarn("Unknown field supplied to CampaignRepository SortParameter: " + options.SortParameter.Name);
                        }
                        else
                        {
                            string sortParameterName = _sortFieldMapping[options.SortParameter.Name];
                            sortParameter = $"{sortParameterName} {options.SortParameter.Direction}";
                        }
                    }

                    command.CommandText = $"SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY {sortParameter}) RowNumber, * FROM {innerCommandText}) x WHERE RowNumber >= {rowFirst} AND RowNumber <= {rowLast} ORDER BY RowNumber";
                    AddSearchParameters(options, command);

                    Logger.LogDebug(command.CommandText);
                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            EmailCampaignsData campaign = ToDomain(reader);
                            results.Results.Add(campaign);
                        }
                    }
                }
            }

            return results;
        }

        private static string GetCampaignInsertCommandText()
        {
            string fields = string.Join(", ", CampaignFields.Select(x => $"[{x}]"));
            string values = string.Join(", ", CampaignFields.Select(x => $"@{x}"));

            return $"INSERT INTO Campaigns([MessageID], {fields}) VALUES(@MessageID, {values})";
        }

        private static string GetCampaignUpdateCommandText()
        {
            string sets = string.Join(", ", CampaignFields.Select(x => $"[{x}]=@{x}"));

            return $"UPDATE [Campaigns] SET {sets} WHERE MessageID=@MessageID";
        }

        private static void AddParameters([NotNull] EmailCampaignsData data, [NotNull] SqlCommand command)
        {
            Assert.ArgumentNotNull(data, nameof(data));
            Assert.ArgumentNotNull(command, nameof(command));

            command.Parameters.Add(new SqlParameter("@MessageID", data.MessageId));
            command.Parameters.Add(new SqlParameter("@ManagerRootID", data.ManagerRootId));
            command.Parameters.Add(new SqlParameter("@CampaignID", data.CampaignId ?? SqlGuid.Null)
            {
                IsNullable = true
            });

            command.Parameters.Add(new SqlParameter("@StartDate", data.StartDate == default(DateTime) ? SqlDateTime.Null : data.StartDate.ToUniversalTime())
            {
                IsNullable = true
            });

            command.Parameters.Add(new SqlParameter("@EndDate", data.EndDate == default(DateTime) || data.EndDate == DateTime.MaxValue ? SqlDateTime.Null : data.EndDate.ToUniversalTime())
            {
                IsNullable = true
            });

            command.Parameters.Add(new SqlParameter("@TotalRecipients", data.TotalRecipients));
            command.Parameters.Add(new SqlParameter("@IncludedRecipients", data.IncludedRecipients));
            command.Parameters.Add(new SqlParameter("@ExcludedRecipients", data.ExcludedRecipients));
            command.Parameters.Add(new SqlParameter("@GloballyExcluded", data.GloballyExcluded));
            command.Parameters.Add(new SqlParameter("@SentRecipients", data.SentRecipients));
            command.Parameters.Add(new SqlParameter("@UnprocessedRecipients", data.UnprocessedRecipients));
            command.Parameters.Add(new SqlParameter("@BouncedRecipients", data.BouncedRecipients));
            command.Parameters.Add(new SqlParameter("@MessageName", data.MessageName ?? SqlString.Null));
            command.Parameters.Add(new SqlParameter("@Subject", data.Subject ?? SqlString.Null));
            command.Parameters.Add(new SqlParameter("@MessageType", (int)data.MessageType));
            command.Parameters.Add(new SqlParameter("@Status", data.Status));
            command.Parameters.Add(new SqlParameter("@CreatedDate", data.CreatedDate?.ToUniversalTime() ?? SqlDateTime.Null)
            {
                IsNullable = true
            });
            command.Parameters.Add(new SqlParameter("@ModifiedDate", data.ModifiedDate?.ToUniversalTime() ?? SqlDateTime.Null)
            {
                IsNullable = true
            });
            command.Parameters.Add(new SqlParameter("@StatusDate", data.StatusDate?.ToUniversalTime() ?? SqlDateTime.Null)
            {
                IsNullable = true
            });
            command.Parameters.Add(new SqlParameter("@Opens", data.Opens));
            command.Parameters.Add(new SqlParameter("@UniqueOpens", data.UniqueOpens));
            command.Parameters.Add(new SqlParameter("@OpenRate", data.OpenRate));
            command.Parameters.Add(new SqlParameter("@Clicks", data.Clicks));
            command.Parameters.Add(new SqlParameter("@UniqueClicks", data.UniqueClicks));
            command.Parameters.Add(new SqlParameter("@ClickRate", data.ClickRate));
            command.Parameters.Add(new SqlParameter("@Value", data.Value));
            command.Parameters.Add(new SqlParameter("@ValuePerEmail", data.ValuePerEmail));
            command.Parameters.Add(new SqlParameter("@ValuePerVisit", data.ValuePerVisit));
            command.Parameters.Add(new SqlParameter("@PageBounces", data.PageBounces));
            command.Parameters.Add(new SqlParameter("@Unsubscribes", data.Unsubscribes));
            command.Parameters.Add(new SqlParameter("@Spams", data.Spams));
            command.Parameters.Add(new SqlParameter("@Browsed", data.Browsed));
            command.Parameters.Add(new SqlParameter("@ScheduledDate", data.ScheduledDate?.ToUniversalTime() ?? SqlDateTime.Null)
            {
                IsNullable = true
            });
            command.Parameters.Add(new SqlParameter("@AbTest", data.AbTest));
        }

        private static EmailCampaignsData ToDomain([NotNull] IDataRecord reader)
        {
            Assert.ArgumentNotNull(reader, nameof(reader));

            var campaign = new EmailCampaignsData();
            campaign.MessageId = (Guid)reader["MessageID"];
            campaign.CampaignId = reader["CampaignID"] is DBNull ? null : (Guid?)reader["CampaignID"];
            campaign.ManagerRootId = (Guid)reader["ManagerRootID"];
            campaign.MessageName = reader["MessageName"] as string;
            campaign.Subject = reader["Subject"] as string;
            campaign.MessageType = (MessageType)Enum.Parse(typeof(MessageType), reader["MessageType"].ToString());
            campaign.CreatedDate = GetUtcDate(reader, "CreatedDate");
            campaign.ModifiedDate = GetUtcDate(reader, "ModifiedDate");
            campaign.StartDate = GetUtcDate(reader, "StartDate") ?? default(DateTime);
            campaign.EndDate = GetUtcDate(reader, "EndDate") ?? DateTime.MaxValue;
            campaign.ScheduledDate = GetUtcDate(reader, "ScheduledDate");
            campaign.TotalRecipients = (int)reader["TotalRecipients"];
            campaign.IncludedRecipients = (int)reader["IncludedRecipients"];
            campaign.ExcludedRecipients = (int)reader["ExcludedRecipients"];
            campaign.GloballyExcluded = (int)reader["GloballyExcluded"];
            campaign.SentRecipients = (int)reader["SentRecipients"];
            campaign.SkippedRecipients = (int)reader["SkippedRecipients"];
            campaign.UnprocessedRecipients = (int)reader["UnprocessedRecipients"];
            campaign.BouncedRecipients = (int)reader["BouncedRecipients"];
            campaign.FailedRecipients = (int)reader["FailedRecipients"];
            campaign.Status = reader["Status"] as string;
            campaign.StatusDate = GetUtcDate(reader, "StatusDate");
            campaign.Opens = (int)reader["Opens"];
            campaign.UniqueOpens = (int)reader["UniqueOpens"];
            campaign.OpenRate = (decimal)reader["OpenRate"];
            campaign.Clicks = (int)reader["Clicks"];
            campaign.UniqueClicks = (int)reader["UniqueClicks"];
            campaign.ClickRate = (decimal)reader["ClickRate"];
            campaign.PageBounces = (int)reader["PageBounces"];
            campaign.Unsubscribes = (int)reader["Unsubscribes"];
            campaign.Spams = (int)reader["Spams"];
            campaign.Browsed = (int)reader["Browsed"];
            campaign.Value = (int)reader["Value"];
            campaign.ValuePerEmail = (decimal)reader["ValuePerEmail"];
            campaign.ValuePerVisit = (decimal)reader["ValuePerVisit"];
            campaign.AbTest = (bool)reader["AbTest"];

            return campaign;
        }

        private static DateTime? GetUtcDate([NotNull] IDataRecord reader, [NotNull] string column)
        {
            Assert.ArgumentNotNull(reader, nameof(reader));
            Assert.ArgumentNotNull(column, nameof(column));

            if (reader[column] is DBNull)
            {
                return null;
            }

            return DateTime.SpecifyKind((DateTime)reader[column], DateTimeKind.Utc);
        }

        private static void AddSortFieldMappings()
        {
            _sortFieldMapping = new Dictionary<string, string>();
            AddSortFieldMapping(x => x.Name, x => x.MessageName);
            AddSortFieldMapping(x => x.Subject, x => x.Subject);
            AddSortFieldMapping(x => x.Created, x => x.CreatedDate);
            AddSortFieldMapping(x => x.StartDate, x => x.StartDate);
            AddSortFieldMapping(x => x.EndDate, x => x.EndDate);
            AddSortFieldMapping(x => x.Scheduled, x => x.ScheduledDate);
            AddSortFieldMapping(x => x.Type, x => x.MessageType);
            AddSortFieldMapping(x => x.ValuePerEmail, x => x.ValuePerEmail);
            AddSortFieldMapping(x => x.ValuePerVisit, x => x.ValuePerVisit);
            AddSortFieldMapping(x => x.Updated, x => x.ModifiedDate);
        }

        private static void AddSortFieldMapping([NotNull] Expression<Func<MessageStatistics, object>> messageStatisticsExpression, [NotNull] Expression<Func<EmailCampaignsData, object>> emailCampaignsDataExpression)
        {
            Assert.ArgumentNotNull(messageStatisticsExpression, nameof(messageStatisticsExpression));
            Assert.ArgumentNotNull(emailCampaignsDataExpression, nameof(emailCampaignsDataExpression));

            string messageStatisticsJsonPropertyName = ReflectionUtil.GetJsonPropertyName(messageStatisticsExpression);
            string emailCampaignsDataPropertyName = ReflectionUtil.GetPropertyName(emailCampaignsDataExpression);

            Assert.IsNotNull(messageStatisticsJsonPropertyName, nameof(messageStatisticsJsonPropertyName));
            Assert.IsNotNull(emailCampaignsDataPropertyName, nameof(emailCampaignsDataPropertyName));

            _sortFieldMapping[messageStatisticsJsonPropertyName] = emailCampaignsDataPropertyName;
        }

        private static void AddSearchParameters([NotNull] CampaignSearchOptions options, [NotNull] SqlCommand command)
        {
            Assert.ArgumentNotNull(options, nameof(options));
            Assert.ArgumentNotNull(command, nameof(command));

            if (options.SentDateFrom.HasValue)
            {
                command.Parameters.Add(new SqlParameter("@StatusDateFrom", options.SentDateFrom));
            }

            if (options.SentDateTo.HasValue)
            {
                command.Parameters.Add(new SqlParameter("@StatusDateTo", options.SentDateTo));
            }

            if (!string.IsNullOrEmpty(options.SearchExpression))
            {
                command.Parameters.Add(new SqlParameter("@SearchExpression", $"%{options.SearchExpression}%"));
            }

            if (options.StatusInclude != null && options.StatusInclude.Any())
            {
                for (var i = 0; i < options.StatusInclude.Length; i++)
                {
                    command.Parameters.Add(new SqlParameter("@includeStatus" + i, options.StatusInclude[i]));
                }
            }

            if (options.StatusExclude != null && options.StatusExclude.Any())
            {
                for (var i = 0; i < options.StatusExclude.Length; i++)
                {
                    command.Parameters.Add(new SqlParameter("@excludeStatus" + i, options.StatusExclude[i]));
                }
            }
        }

        internal static List<string> GetUpdateCampaignCommandDateFields(bool hasStartDate, bool hasEndDate)
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

        internal static string GetUpdateCampaignCommandTextRecipientField(string fieldName, FieldUpdate<int> field)
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

        #endregion;

        #region Dispatch Queue

        /// <inheritdoc />
        public override void AddToDispatchQueue(Guid messageId, MessageType messageType, [NotNull] IEnumerable<Tuple<string, DispatchType>> recipients, Dictionary<string, object> customPersonTokens = null)
        {
            // Input Validation
            Assert.ArgumentNotNull(recipients, "recipients");

            // Add to queue.
            var queueItems = recipients.Select(
                q => new DispatchQueueItem
                {
                    MessageId = messageId,
                    RecipientId = q.Item1,
                    RecipientQueue = q.Item2 == DispatchType.AbTest ? RecipientQueue.AbTestRecipient : RecipientQueue.Recipient,
                    DispatchType = q.Item2,
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

        /// <inheritdoc />
        public override void ClearDispatchQueue(Guid messageId)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DELETE FROM [DispatchQueue] WHERE [MessageID] = @MessageID";
                    command.Parameters.Add(new SqlParameter("@MessageID", messageId));
                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <inheritdoc />
        public override long CountRecipientsInDispatchQueue(Guid messageId, params RecipientQueue[] queueStates)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT COUNT([ID]) FROM [DispatchQueue] WHERE [MessageId] = @MessageId";

                    if (queueStates != null && queueStates.Length > 0)
                    {
                        command.CommandText += string.Format(" AND [RecipientQueue] IN ({0})", string.Join(",", queueStates.Cast<int>().ToArray()));
                    }

                    command.Parameters.Add(new SqlParameter("@MessageId", messageId));
                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;

                    return System.Convert.ToInt64(command.ExecuteScalar());
                }
            }
        }

        /// <inheritdoc />
        public override void DeleteRecipientsFromDispatchQueue(Guid queueItemId)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DELETE FROM [DispatchQueue] WHERE [ID] = @ID";
                    command.Parameters.Add(new SqlParameter("@ID", queueItemId));
                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <inheritdoc />
        public override IEnumerable<Guid> GetMessagesInProgress(MessageType messageType, TimeSpan timeout)
        {
            var messageIds = new List<Guid>();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT DISTINCT([MessageID]) FROM [DispatchQueue] WHERE [MessageType] = @MessageType AND [InProgress] = 1 AND [LastModified] < @LastModified";
                    command.Parameters.Add(new SqlParameter("@MessageType", messageType));
                    command.Parameters.Add(new SqlParameter("@LastModified", DateTime.UtcNow.Subtract(timeout)));
                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;

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

        /// <inheritdoc />
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
                        "OUTPUT inserted.ID, inserted.MessageID, inserted.RecipientID, inserted.RecipientQueue, inserted.DispatchType, " +
                        "inserted.LastModified, inserted.MessageType, inserted.CustomPersonTokens, inserted.InProgress " +
                        "WHERE [InProgress] = 0";

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

                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
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

        /// <inheritdoc />
        public override void ResetProcessState(Guid messageId, TimeSpan? timeout = null)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "UPDATE [DispatchQueue] SET [InProgress] = 0, [LastModified] = @LastModified " +
                        "WHERE [MessageID] = @MessageID AND [InProgress] = 1";
                    command.Parameters.Add(new SqlParameter("@LastModified", DateTime.UtcNow));
                    command.Parameters.Add(new SqlParameter("@MessageID", messageId));

                    if (timeout == default(TimeSpan))
                    {
                        command.CommandText += " AND [LastModified] < @Timeout";
                        command.Parameters.Add(new SqlParameter("@Timeout", DateTime.UtcNow.Subtract(timeout.Value)));
                    }

                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <inheritdoc />
        public override void ChangeDispatchType(Guid messageId, DispatchType sourceType, DispatchType targetType, int itemCount)
        {
            var sourceQueue = sourceType == DispatchType.AbTest ? RecipientQueue.AbTestRecipient : RecipientQueue.Recipient;
            var targetQueue = targetType == DispatchType.AbTest ? RecipientQueue.AbTestRecipient : RecipientQueue.Recipient;

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "UPDATE TOP(@ItemCount) [DispatchQueue] " +
                        "SET [RecipientQueue] = @TargetQueue, [DispatchType] = @TargetType, [LastModified] = @LastModified " +
                        "WHERE [MessageID] = @MessageID AND [RecipientQueue] = @SourceQueue AND [InProgress] = 0";

                    command.Parameters.Add(new SqlParameter("@ItemCount", itemCount));
                    command.Parameters.Add(new SqlParameter("@TargetQueue", targetQueue));
                    command.Parameters.Add(new SqlParameter("@TargetType", targetType));
                    command.Parameters.Add(new SqlParameter("@LastModified", DateTime.UtcNow));
                    command.Parameters.Add(new SqlParameter("@MessageID", messageId));
                    command.Parameters.Add(new SqlParameter("@SourceQueue", sourceQueue));
                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;

                    command.ExecuteNonQuery();
                }
            }
        }

        /// <inheritdoc />
        public override void DeleteCampaign(Guid messageId)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = "DELETE FROM Campaigns WHERE MessageID=@MessageID";
                    command.Parameters.Add(new SqlParameter("@MessageID", messageId));
                    command.CommandTimeout = (int)_commandTimeout.TotalMilliseconds;
                    command.ExecuteNonQuery();
                }
            }
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
                RecipientQueue = (RecipientQueue)dataReader.GetByte(3),
                DispatchType = (DispatchType)dataReader.GetByte(4),
                LastModified = dataReader.GetDateTime(5),
                MessageType = (MessageType)dataReader.GetByte(6),
                CustomPersonTokens = JsonConvert.DeserializeObject<Dictionary<string, object>>(dataReader.IsDBNull(7) ? string.Empty : dataReader.GetString(7)),
                InProgress = dataReader.GetBoolean(8)
            };
        }

        #endregion
    }
}
