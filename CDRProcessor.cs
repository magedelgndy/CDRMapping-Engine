using ConnekioMarketingTool.Application.Responses.CDRMapping;
using ConnekioMarketingTool.Core.Entities;
using ConnekioMarketingTool.Core.Entities.CDRMapping;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CDRMappingEngine
{
    public class CDRProcessor
    {
        private readonly string _connectionString;
        private readonly ILogger<Worker> _logger;

        public CDRProcessor(string connectionString, ILogger<Worker> logger)
        {
            _connectionString = connectionString;
            _logger = logger;
        }

        public async Task ProcessBatchCDRsAsync(List<CDRInfoResponse> batch)
        {
            if (batch == null || batch.Count == 0)
                return;

            Stopwatch stopwatch = Stopwatch.StartNew();
            var dataTable = new DataTable();

            dataTable.Columns.Add("ContactId", typeof(int));
            dataTable.Columns.Add("CDRTypeId", typeof(int));
            dataTable.Columns.Add("ApplicationId", typeof(int));
            dataTable.Columns.Add("CDRAttributeId", typeof(int));
            dataTable.Columns.Add("UserAttributeId", typeof(int));
            dataTable.Columns.Add("EventId", typeof(int));
            dataTable.Columns.Add("IndexNumber", typeof(int));
            dataTable.Columns.Add("CDRAttributeName", typeof(string));
            dataTable.Columns.Add("CDRAttributeValue", typeof(string));
            dataTable.Columns.Add("CDRSubAttributeValue", typeof(string));
            dataTable.Columns.Add("FileId", typeof(Guid));
            dataTable.Columns.Add("InsertionDate", typeof(DateTime));
            dataTable.Columns.Add("CreationDate", typeof(DateTime));
            dataTable.Columns.Add("RowUniqueIdentifier", typeof(Guid));
            dataTable.Columns.Add("IsRowFinished", typeof(bool));
            dataTable.Columns.Add("PeeOfferId", typeof(string));

            foreach (var item in batch)
            {
                Guid fileId;
                Guid rowUniqueId;

                // Defensive Guid parsing
                if (!Guid.TryParse(item.FileId.ToString(), out fileId))
                {
                    _logger.LogError($"Invalid FileId for contact {item.ContactId} - skipping record.");
                    continue;
                }

                if (!Guid.TryParse(item.RowUniqueIdentifier.ToString(), out rowUniqueId))
                {
                    _logger.LogError($"Invalid RowUniqueIdentifier for contact {item.ContactId} - skipping record.");
                    continue;
                }

                dataTable.Rows.Add(
                    item.ContactId,
                    item.CDRTypeId,
                    item.ApplicationId,
                    item.CDRAttributeId,
                    (object?)item.UserAttributeId ?? DBNull.Value,
                    (object?)item.EventId ?? DBNull.Value,
                    item.IndexNumber,
                    item.CDRAttributeName ?? string.Empty,
                    item.CDRAttributeValue ?? string.Empty,
                    item.CDRSubAttributeValue ?? string.Empty,
                    fileId,                       // Safe Guid
                    item.InsertionDate,
                    item.CreationDate ?? DateTime.Now,
                    rowUniqueId,                 // Safe Guid
                    item.IsRowFinished ?? false,
                    item.PeeOfferId ?? string.Empty
                );
            }


            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = "CDRInfo",
                BatchSize = batch.Count
            };

            bulkCopy.ColumnMappings.Add("ContactId", "ContactId");
            bulkCopy.ColumnMappings.Add("CDRTypeId", "CDRTypeId");
            bulkCopy.ColumnMappings.Add("ApplicationId", "ApplicationId");
            bulkCopy.ColumnMappings.Add("CDRAttributeId", "CDRAttributeId");
            bulkCopy.ColumnMappings.Add("UserAttributeId", "UserAttributeId");
            bulkCopy.ColumnMappings.Add("EventId", "EventId");
            bulkCopy.ColumnMappings.Add("IndexNumber", "IndexNumber");
            bulkCopy.ColumnMappings.Add("CDRAttributeName", "CDRAttributeName");
            bulkCopy.ColumnMappings.Add("CDRAttributeValue", "CDRAttributeValue");
            bulkCopy.ColumnMappings.Add("CDRSubAttributeValue", "CDRSubAttributeValue");
            bulkCopy.ColumnMappings.Add("FileId", "FileId");
            bulkCopy.ColumnMappings.Add("InsertionDate", "InsertionDate");
            bulkCopy.ColumnMappings.Add("CreationDate", "CreationDate");
            bulkCopy.ColumnMappings.Add("RowUniqueIdentifier", "RowUniqueIdentifier");
            bulkCopy.ColumnMappings.Add("IsRowFinished", "IsRowFinished");
            bulkCopy.ColumnMappings.Add("PeeOfferId", "PeeOfferId");

            await bulkCopy.WriteToServerAsync(dataTable);
            stopwatch.Stop();
            _logger.LogInformation($"Successfully inserted {batch.Count} records into CDRInfo table.AND Take {stopwatch.ElapsedMilliseconds} ms");
        }

    }
}
