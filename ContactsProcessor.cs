using ConnekioMarketingTool.Core.Entities;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace CDRMappingEngine
{
    public class ContactsProcessor
    {
        private readonly string _connectionString;
        private readonly ILogger<Worker> _logger;

        public ContactsProcessor(string connectionString, ILogger<Worker> logger)
        {
            _connectionString = connectionString;
            _logger = logger;
        }

        public async Task ProcessBatchSubscribersAsync(List<Contact> batch)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
                    {
                        try
                        {
                            // Step 1: Create temporary table
                            string createTempTableQuery = @"
                                CREATE TABLE #StagingContacts (
                                    PhoneNumber NVARCHAR(50),
                                    RedBullSubId NVARCHAR(MAX),
                                    EnterpriseUserId NVARCHAR(100),
                                    ApplicationId INT
                                );";
                            using (var createTempCmd = new SqlCommand(createTempTableQuery, connection, transaction))
                            {
                                await createTempCmd.ExecuteNonQueryAsync();
                            }

                            // Step 2: Bulk insert into temp table
                            await BulkInsertAsync(connection, transaction, batch);

                            // Step 3: MERGE with UPDLOCK + HOLDLOCK
                            string mergeQuery = @"
                                MERGE INTO Contacts WITH (UPDLOCK, HOLDLOCK) AS Target
                                USING #StagingContacts AS Source
                                ON Target.PhoneNumber = Source.PhoneNumber AND Target.RedBullSubId = Source.RedBullSubId
                                WHEN MATCHED THEN
                                    UPDATE SET 
                                        EnterpriseUserId = Source.EnterpriseUserId,
                                        ApplicationId = Source.ApplicationId,
                                        UpdatedAt = GETDATE()
                                WHEN NOT MATCHED THEN
                                    INSERT (PhoneNumber, RedBullSubId, EnterpriseUserId, ApplicationId, InsertionDate, Active)
                                    VALUES (Source.PhoneNumber, Source.RedBullSubId, Source.EnterpriseUserId, Source.ApplicationId, GETDATE(), 1);";
                            using (var mergeCmd = new SqlCommand(mergeQuery, connection, transaction))
                            {
                                await mergeCmd.ExecuteNonQueryAsync();
                            }

                            // Step 4: Drop temp table
                            string dropTempTableQuery = "DROP TABLE #StagingContacts;";
                            using (var dropTempCmd = new SqlCommand(dropTempTableQuery, connection, transaction))
                            {
                                await dropTempCmd.ExecuteNonQueryAsync();
                            }

                            await transaction.CommitAsync();
                        }
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync();
                            _logger.LogError(ex, "Error in ProcessBatchSubscribersAsync");
                            throw;
                        }
                    }
                }
            });
        }

        public async Task ProcessBatchCustomersAsync(List<Contact> batch)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
                    {
                        try
                        {
                            // Step 1: Create temporary table
                            string createTempTableQuery = @"
                                CREATE TABLE #StagingContacts (
                                    FirstName NVARCHAR(50),
                                    LastName NVARCHAR(MAX),
                                    EnterpriseUserId NVARCHAR(100)
                                );";
                            using (var createTempCmd = new SqlCommand(createTempTableQuery, connection, transaction))
                            {
                                await createTempCmd.ExecuteNonQueryAsync();
                            }

                            // Step 2: Bulk insert into temp table
                            await BulkInsertCustomerAsync(connection, transaction, batch);

                            // Step 3: MERGE with UPDLOCK + HOLDLOCK
                            string mergeQuery = @"
                                MERGE INTO Contacts WITH (UPDLOCK, HOLDLOCK) AS Target
                                USING #StagingContacts AS Source
                                ON Target.EnterpriseUserId = Source.EnterpriseUserId
                                WHEN MATCHED THEN
                                    UPDATE SET 
                                        FirstName = Source.FirstName,
                                        LastName = Source.LastName,
                                        UpdatedAt = GETDATE();";
                            using (var mergeCmd = new SqlCommand(mergeQuery, connection, transaction))
                            {
                                await mergeCmd.ExecuteNonQueryAsync();
                            }

                            // Step 4: Drop temp table
                            string dropTempTableQuery = "DROP TABLE #StagingContacts;";
                            using (var dropTempCmd = new SqlCommand(dropTempTableQuery, connection, transaction))
                            {
                                await dropTempCmd.ExecuteNonQueryAsync();
                            }

                            await transaction.CommitAsync();
                        }
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync();
                            _logger.LogError(ex, "Error in ProcessBatchCustomersAsync");
                            throw;
                        }
                    }
                }
            });
        }

        private async Task BulkInsertAsync(SqlConnection connection, SqlTransaction transaction, List<Contact> batch)
        {
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, transaction)) // Notice TableLock and Transaction
            {
                bulkCopy.DestinationTableName = "#StagingContacts";

                var table = new DataTable();
                table.Columns.Add("PhoneNumber", typeof(string));
                table.Columns.Add("RedBullSubId", typeof(string));
                table.Columns.Add("EnterpriseUserId", typeof(string));
                table.Columns.Add("ApplicationId", typeof(int));

                foreach (var contact in batch)
                {
                    table.Rows.Add(contact.PhoneNumber, contact.RedBullSubId, contact.EnterpriseUserId, contact.ApplicationId);
                }

                await bulkCopy.WriteToServerAsync(table);
            }
        }

        private async Task BulkInsertCustomerAsync(SqlConnection connection, SqlTransaction transaction, List<Contact> batch)
        {
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, transaction))
            {
                bulkCopy.DestinationTableName = "#StagingContacts";

                var table = new DataTable();
                table.Columns.Add("FirstName", typeof(string));
                table.Columns.Add("LastName", typeof(string));
                table.Columns.Add("EnterpriseUserId", typeof(string));

                foreach (var contact in batch)
                {
                    table.Rows.Add(contact.FirstName, contact.LastName, contact.EnterpriseUserId);
                }

                await bulkCopy.WriteToServerAsync(table);
            }
        }

        private async Task ExecuteWithRetryAsync(Func<Task> action, int maxRetries = 10)
        {
            int retryCount = 0;
            while (true)
            {
                try
                {
                    await action();
                    return;
                }
                catch (SqlException ex) when (ex.Number == 1205) // Deadlock detected
                {
                    retryCount++;
                    _logger.LogWarning($"Deadlock detected. Retry attempt {retryCount}/{maxRetries}.");
                    if (retryCount >= maxRetries)
                        throw;
                    await Task.Delay(500 * retryCount); // Exponential backoff
                }
            }
        }

        private List<List<Contact>> SplitIntoBatches(List<Contact> contacts, int batchSize)
        {
            var batches = new List<List<Contact>>();
            for (int i = 0; i < contacts.Count; i += batchSize)
            {
                batches.Add(contacts.GetRange(i, Math.Min(batchSize, contacts.Count - i)));
            }
            return batches;
        }
    }
}
