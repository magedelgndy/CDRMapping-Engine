using CDRMappingEngine.Redis;
using ConnekioMarketingTool.Application.Command.Accounts;
using ConnekioMarketingTool.Application.Command.CDRInfo;
using ConnekioMarketingTool.Application.Command.CDRMapping;
using ConnekioMarketingTool.Application.Command.Contacts;
using ConnekioMarketingTool.Application.Handlers.Query.CDRMapping;
using ConnekioMarketingTool.Application.Model;
using ConnekioMarketingTool.Application.Model.WorkFlowModel;
using ConnekioMarketingTool.Application.Query;
using ConnekioMarketingTool.Application.Query.BundleService;
using ConnekioMarketingTool.Application.Query.CDRAttribute;
using ConnekioMarketingTool.Application.Query.CDRInfo;
using ConnekioMarketingTool.Application.Query.Contacts;
using ConnekioMarketingTool.Application.Query.Events;
using ConnekioMarketingTool.Application.Query.PeeFreeUnit;
using ConnekioMarketingTool.Application.Query.SubAttribute;
using ConnekioMarketingTool.Application.Query.UserAttribute;
using ConnekioMarketingTool.Application.Responses;
using ConnekioMarketingTool.Application.Responses.CDRMapping;
using ConnekioMarketingTool.Application.Responses.OperatorsPlatform;
using ConnekioMarketingTool.Core.Entities;
using ConnekioMarketingTool.Core.Entities.OperatorsPlatform;
using ConnekioMarketingTool.Core.Enums;
using ConnekioMarketingTool.Core.Models;
using Elsa.Models;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using Newtonsoft.Json;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using static System.Formats.Asn1.AsnWriter;
using Stopwatch = System.Diagnostics.Stopwatch;

namespace CDRMappingEngine
{
    public class Worker : BackgroundService
    {
        private int counter = 1;
        private readonly ILogger<Worker> _logger;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IConfiguration _configuration;
        private static ConcurrentDictionary<string, bool> _processedFiles = new();
        private readonly IMemoryCache _memoryCache;
        private readonly IRedisCacheService _redisCache;


        public Worker(ILogger<Worker> logger, IServiceScopeFactory scopeFactory, IConfiguration configuration,IMemoryCache memoryCache, IRedisCacheService redisCache)
        {
            _logger = logger;
            _redisCache = redisCache;
            _scopeFactory = scopeFactory;
            _configuration = configuration;
            _memoryCache = memoryCache;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation("RedBull CDR Engine start working at: {time}", DateTimeOffset.Now);
                    int engineNumber = int.Parse(_configuration["AppSettings:EngineNumber"] ?? "2");
                    int isReloadCache = int.Parse(_configuration["AppSettings:isReloadCache"] ?? "0");
                    _logger.LogInformation($"Engine {engineNumber} starting... Processor Count: {Environment.ProcessorCount}, Thread Count: {Process.GetCurrentProcess().Threads.Count}");
                    if (isReloadCache == 1) {
                        await GetAllPeeFreeUnitCachedAsync();
                    }
                    
                    using (var scope = _scopeFactory.CreateScope())
                    {
                        var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

                        var cDRFileToProcessResponse = await mediator.Send(new GetFilesToProcessByEngineNumberQuery()
                        {
                            EngineNumber = engineNumber
                        });

                        await StartProcessFiles(mediator, cDRFileToProcessResponse);

                        await CheckFileMetadata(mediator);
                    }

                    _logger.LogInformation("CDR Engine end working at: {time}", DateTimeOffset.Now);
                    await Task.Delay(TimeSpan.FromMinutes(2), stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message, "An error occurred while executing the worker.");
                await Task.Delay(500, stoppingToken);
            }
        }

        #region Insert File Metadata

        private async Task CheckFileMetadata(IMediator mediator)
        {
            try
            {
                int engineNumber = int.Parse(_configuration["AppSettings:EngineNumber"] ?? "1");
                int peeFreeUnitTypeId = int.Parse(_configuration["AppSettings:PeeFreeUnitTypeId"] ?? "15");

                _logger.LogInformation("Get Folders Path");
                var folderPaths = await mediator.Send(new GetAllFoldersDataQuery() { EngineNumber = engineNumber });
                var maxBatchSize = int.Parse(_configuration["AppSettings:FolderBatchSize"] ?? "10000");
                int counterFolder = 1;

                Stopwatch sw = Stopwatch.StartNew();

                foreach (var folder in folderPaths)
                {

                    _logger.LogInformation($"Check New CDR Files On {folder.Path}");

                    if (!Directory.Exists(folder.Path))
                        continue;

                    _logger.LogInformation($"Start Collect files from Directory to check MetaData On {folder.Path}");
                    var files = Directory.GetFiles(folder.Path);

                    var existingFileNames = new ConcurrentDictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                    var cdrFileCommands = new ConcurrentBag<CDRFileResponse>(); // Thread-safe collection for parallel processing

                    //var batchSemaphore = new SemaphoreSlim(InitialSemaCount, MaxSemaCount); // Semaphore to manage batch insertions
                    //var options = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * FilesProcessorCountMultipllied }; // Reduce task switching overhead by setting a MaxDegreeOfParallelism that balances CPU usage and I/O.
                    //await Parallel.ForEachAsync(files, async (file, cancellationToken) =>
                    //{
                    //using var scope = _scopeFactory.CreateScope();
                    //var scopedMediator = scope.ServiceProvider.GetRequiredService<IMediator>();

                    foreach (var file in files)
                    {

                        Interlocked.Increment(ref counterFolder);
                        var fileName = Path.GetFileName(file);

                        if (existingFileNames.ContainsKey(fileName))
                        {
                            _logger.LogInformation($"File {fileName} already in cache, skipping...");
                            return;
                        }

                        if (!_processedFiles.TryAdd(fileName, true)) // to validate file processed once
                        {
                            _logger.LogInformation($"File {fileName} is already being processed sooo skipped");
                            return;

                        }
                        existingFileNames.TryAdd(fileName, true); // Update cache

                        var existingFile = await mediator.Send(new GetCDRFileByFileNameQuery { FileName = fileName });

                        if (existingFile == null)
                        {
                            var cdrFileResponse = new CDRFileResponse
                            {
                                CDRFolderId = folder.Id,
                                CDRTypeId = folder.CDRTypeId,
                                EngineNumber = engineNumber,
                                CDRFileName = fileName,
                                IsProcessing = false,
                                ProcessingResponse = ProcessingResponseEnum.pending,
                                InsertionDate = DateTime.Now,
                                ExistencePath = file
                            };

                            cdrFileCommands.Add(cdrFileResponse);
                            if (cdrFileCommands.Count >= maxBatchSize)
                            {

                                var batch = cdrFileCommands.Take(maxBatchSize).Distinct().ToList(); // Take a batch
                                foreach (var command in batch) // Remove processed files from the bag
                                {
                                    cdrFileCommands.TryTake(out _);
                                }

                                sw.Restart();
                                var cDRFiles = await mediator.Send(new CreateCDRFilesRangeCommand { CdrFiles = batch });
                                _logger.LogInformation($"Finish Inserting batch of {batch.Count} files metadata in {sw.ElapsedMilliseconds}");

                                sw.Restart();
                                _logger.LogInformation($"Finish Process batch of {cDRFiles.Count} files on type {folder.CDRTypeId} in {sw.ElapsedMilliseconds}");
                            }
                        }
                        else if (existingFile.ProcessingResponse == ProcessingResponseEnum.Failure)
                        {
                            CDRFileToProcessResponse cDRFileToProcessResponse = new CDRFileToProcessResponse
                            {
                                Id = existingFile.Id,
                                CDRFolderId = existingFile.CDRFolderId,
                                CDRTypeId = existingFile.CDRTypeId,
                                CDRFileName = existingFile.CDRFileName,
                                ExistencePath = file
                            };
                            MoveFileToFailedFolder(cDRFileToProcessResponse);
                        }
                        else if (existingFile.IsSucceeded == true && existingFile.ProcessingResponse == ProcessingResponseEnum.Success)
                        {
                            CDRFileToProcessResponse cDRFileToProcessResponse = new CDRFileToProcessResponse
                            {
                                Id = existingFile.Id,
                                CDRFolderId = existingFile.CDRFolderId,
                                CDRTypeId = existingFile.CDRTypeId,
                                CDRFileName = existingFile.CDRFileName,
                                ExistencePath = file
                            };
                            MoveFileToProcessedFolder(cDRFileToProcessResponse);
                        }
                    }

                    // Insert any remaining files after the parallel loop
                    if (cdrFileCommands.Any())
                    {
                        var batch = cdrFileCommands.Take(cdrFileCommands.Count).Distinct().ToList(); // Take a batch

                        cdrFileCommands.Clear();

                        sw.Restart();
                        var cDRFiles = mediator.Send(new CreateCDRFilesRangeCommand { CdrFiles = batch }).Result;
                        _logger.LogInformation($"Finish Inserting batch of {batch.Count} files metadata in {sw.ElapsedMilliseconds}");

                        sw.Restart();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error in Insert File Metadata method: {ex.Message}");
            }
        }

        #endregion

        #region Process File
        private async Task StartProcessFiles(IMediator mediator, List<CDRFileToProcessResponse> cDRFileToProcessResponse)
        {
            if (cDRFileToProcessResponse == null || cDRFileToProcessResponse.Count == 0)
            {
                _logger.LogInformation("No new CDR files to process.");
                return;
            }

            int peeFreeUnitTypeId = int.Parse(_configuration["AppSettings:PeeFreeUnitTypeId"] ?? "15");
            var stopwatch = new Stopwatch();
            int initialSemaCount = int.Parse(_configuration["AppSettings:InitialSemaCount"] ?? "1");
            int maxSemaCount = int.Parse(_configuration["AppSettings:MaxSemaCount"] ?? "3");
            var semaphore = new SemaphoreSlim(initialSemaCount, maxSemaCount);

            _logger.LogInformation($"Starting to process {cDRFileToProcessResponse.Count} CDR files...");

            await Parallel.ForEachAsync(cDRFileToProcessResponse, async (cdrFile, cancellationToken) =>
            {
                await semaphore.WaitAsync(cancellationToken);
                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var scopedMediator = scope.ServiceProvider.GetRequiredService<IMediator>();

                    _logger.LogInformation($"Start processing CDR file '{cdrFile.CDRFileName}' (FolderId: {cdrFile.CDRFolderId}) at {DateTimeOffset.Now}");
                    stopwatch.Restart();

                    if (cdrFile.CDRTypeId == peeFreeUnitTypeId)
                    {
                        _logger.LogInformation($"Start Working on CDR {cdrFile.CDRFileName} FolderId {cdrFile.CDRFolderId} at: {DateTimeOffset.Now}");
                        stopwatch.Restart();
                        await ProcessPeeFreeUnitFilesAsync(cdrFile, scopedMediator);
                        _logger.LogInformation($"Process File Working on CDR {cdrFile.CDRFileName} FolderId {cdrFile.CDRFolderId} take: {stopwatch.ElapsedMilliseconds}");
                    }
                    else
                    {
                        _logger.LogInformation($"Start Working on CDR {cdrFile.CDRFileName} FolderId {cdrFile.CDRFolderId} at: {DateTimeOffset.Now}");
                        stopwatch.Restart();
                        await ProcessFileSalahAsync(cdrFile, scopedMediator);
                        _logger.LogInformation($"Process File Working on CDR {cdrFile.CDRFileName} FolderId {cdrFile.CDRFolderId} take: {stopwatch.ElapsedMilliseconds}");
                    }
                    stopwatch.Stop();
                    _logger.LogInformation($"Completed processing CDR file '{cdrFile.CDRFileName}' (FolderId: {cdrFile.CDRFolderId}) in {stopwatch.ElapsedMilliseconds} ms");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Error processing CDR file '{cdrFile.CDRFileName}' (FolderId: {cdrFile.CDRFolderId})");
                }
                finally
                {
                    semaphore.Release();
                }
            });

            _logger.LogInformation("Finished processing all CDR files.");

            //foreach (var cdrFile in cDRFileToProcessResponse)
            //{

            //    if (cdrFile.CDRTypeId == peeFreeUnitTypeId)
            //    {
            //        _logger.LogInformation($"Start Working on CDR {cdrFile.CDRFileName} FolderId {cdrFile.CDRFolderId} at: {DateTimeOffset.Now}");
            //        stopwatch.Restart();
            //        await ProcessPeeFreeUnitFilesAsync(cdrFile, mediator);
            //        _logger.LogInformation($"Process File Working on CDR {cdrFile.CDRFileName} FolderId {cdrFile.CDRFolderId} take: {stopwatch.ElapsedMilliseconds}");
            //    }
            //    else
            //    {
            //        _logger.LogInformation($"Start Working on CDR {cdrFile.CDRFileName} FolderId {cdrFile.CDRFolderId} at: {DateTimeOffset.Now}");
            //        stopwatch.Restart();
            //        await ProcessFileSalahAsync(cdrFile, mediator);
            //        _logger.LogInformation($"Process File Working on CDR {cdrFile.CDRFileName} FolderId {cdrFile.CDRFolderId} take: {stopwatch.ElapsedMilliseconds}");
            //    }
            //}
            ;
        }
        private async Task ProcessFileSalahAsync(CDRFileToProcessResponse cdrFile, IMediator mediator)
        {

            if (!FileLock.LockFile(cdrFile.ExistencePath))
            {
                _logger.LogWarning("File is already being processed: {file}", cdrFile.CDRFileName);
                return;
            }

            if (!File.Exists(cdrFile.ExistencePath))
            {
                _logger.LogWarning("File does not exist or has already been processed: {file}", cdrFile.CDRFileName);
                return;
            }

            ConcurrentDictionary<string, bool> processedCdrIds = new();

            int counter = 0;
            int batchSize = int.Parse(_configuration["AppSettings:CDRInfoBatchSize"] ?? "10000"); // Batch size for inserting records
            ConcurrentBag<CDRInfoResponse> validCdrInfos = new ConcurrentBag<CDRInfoResponse>(); // Thread-safe collection for batch processing
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var ReadDate = DateTime.Now;
            var totalStopwatch = System.Diagnostics.Stopwatch.StartNew(); // Track total processing time
            try
            {
                int applicationId = int.Parse(_configuration["AppSettings:RedBullAppId"]);
                int InitialSemaCount = int.Parse(_configuration["AppSettings:InitialSemaCount"] ?? "2");
                int MaxSemaCount = int.Parse(_configuration["AppSettings:MaxSemaCount"] ?? "4");

                stopwatch.Restart();

                await mediator.Send(new UpdateCDRFileStatusCommand
                {
                    FileId = cdrFile.Id,
                    IsProcessing = true,
                    ProcessingResponse = ProcessingResponseEnum.ProcessStarted,
                    ReadDate = ReadDate,
                });

                _logger.LogInformation(message: $"Update CDRFile Status completed in {stopwatch.ElapsedMilliseconds} ms for file {cdrFile.CDRFileName}");

                var lines = File.ReadAllLines(cdrFile.ExistencePath, Encoding.UTF8);

                // Step 1: Process Contacts if CDRTypeId is 6 or 8
                if (cdrFile.CDRTypeId == 8 || cdrFile.CDRTypeId == 6)
                {
                    stopwatch.Restart();
                    await ProcessContactsSalahAsync(cdrFile, lines, mediator);
                    _logger.LogInformation(message: $"Contact processing completed in {stopwatch.ElapsedMilliseconds} ms for file {cdrFile.CDRFileName}");
                    // Refresh the contacts cache
                    //await RefreshContactsCacheAsync(mediator);
                }

                // Step 2: Pre-fetch attributes outside the parallel loop
                var attributeStopwatch = System.Diagnostics.Stopwatch.StartNew();
                var cdrAttributes = await GetCDRAttributesAsync(cdrFile.CDRTypeId, mediator);
                var userAttributes = await GetUserAttributesAsync(cdrFile.CDRTypeId, mediator);
                attributeStopwatch.Stop();
                _logger.LogInformation($"Fetched attributes in {attributeStopwatch.ElapsedMilliseconds} ms for file {cdrFile.CDRFileName}");

                EventIdAndName eventRechageAndRecurring = new EventIdAndName(0, "");


                if (cdrFile.CDRTypeId == 4 || cdrFile.CDRTypeId == 5 || cdrFile.CDRTypeId == 13)
                {
                    eventRechageAndRecurring = await GetEventIdAndNameForRechargeAndRecurring(cdrFile.CDRTypeId, mediator);
                }
                //var parallelOptions = new ParallelOptions
                //{
                //    MaxDegreeOfParallelism = int.Parse(_configuration["AppSettings:MaxParallelism"] ?? "3")
                //};
                // Step 3: Process Lines in Parallel
                await Parallel.ForEachAsync(lines, async (line, token) =>
                {

                    using var scope = _scopeFactory.CreateScope(); // Creates a new DI scope for each task
                    var scopedMediator = scope.ServiceProvider.GetRequiredService<IMediator>();

                    Interlocked.Increment(ref counter);
                    //_logger.LogInformation($"Read Line Number {counter} in file {cdrFile.CDRFileName}");
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        _logger.LogInformation($"Skipped empty line {counter} in file {cdrFile.CDRFileName}");
                        return;
                    }

                    attributeStopwatch.Restart();
                    var cdrInfos = await ProcessLinesAsync(line, cdrFile, cdrAttributes, userAttributes, eventRechageAndRecurring, processedCdrIds, scopedMediator);

                    _logger.LogInformation($"Process Lines take {attributeStopwatch.ElapsedMilliseconds} ms ");

                    foreach (var item in cdrInfos)
                    {
                        validCdrInfos.Add(item);
                    }
                });


                // Final batch insertion
                if (validCdrInfos.Any())
                {
                    attributeStopwatch.Restart();
                    var batches = SplitIntoBatches(validCdrInfos.Distinct().ToList(), batchSize);
                    if (batches == null)
                    {
                        _logger.LogInformation($"No valid CdrInfos to insert in Batches for file {cdrFile.CDRFileName}");
                        return;
                    }
                    foreach (var batch in batches)
                    {
                        await InsertBatchSafelyAsync(batch, cdrFile, mediator);

                    }
                    _logger.LogInformation($"Insert Batchs attributes take {attributeStopwatch.ElapsedMilliseconds} ms ");

                }

                await mediator.Send(new UpdateCDRFileStatusCommand
                {
                    FileId = cdrFile.Id,
                    IsProcessing = false,
                    ProcessingResponse = ProcessingResponseEnum.Success,
                    FinishProcessingDate = DateTime.Now,
                    ReadDate = ReadDate,
                    ProcessingTime = (DateTime.Now - ReadDate).TotalMinutes.ToString("F2"),
                    NumberOfInsertedAttributes = (userAttributes.Count * counter),
                    LastInsertedRow = counter,
                    IsSucceeded = true
                });

                MoveFileToProcessedFolder(cdrFile);
                _logger.LogInformation($"Finished processing file {cdrFile.CDRFileName} in {totalStopwatch.ElapsedMilliseconds} ms");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error processing file {cdrFile.CDRFileName}");
                await mediator.Send(new UpdateCDRFileStatusCommand
                {
                    FileId = cdrFile.Id,
                    IsProcessing = false,
                    ProcessingResponse = ProcessingResponseEnum.Failure,
                    ExceptionMessage = ex.Message,
                    FinishProcessingDate = DateTime.Now,
                    ReadDate = ReadDate,
                    ProcessingTime = (DateTime.Now - ReadDate).TotalMinutes.ToString("F2"),
                    LastInsertedRow = counter,
                    IsSucceeded = false
                });
                MoveFileToFailedFolder(cdrFile);
            }
            finally
            {
                FileLock.UnlockFile(cdrFile.ExistencePath);
            }
        }
        public static List<List<T>> SplitIntoBatches<T>(List<T> source, int batchSize)
        {
            if (batchSize <= 0)
            {
                return null;
            }

            return source
                .Select((item, index) => new { item, index })
                .GroupBy(x => x.index / batchSize)
                .Select(group => group.Select(x => x.item).ToList())
                .ToList();
        }
        private async Task InsertBatchSafelyAsync(List<CDRInfoResponse> cdrInfos, CDRFileToProcessResponse cdrFile, IMediator mediator)
        {
            string connectionStr = _configuration.GetConnectionString("ConnekioNoCodeCDRMappingConnection");

            var batchToInsert = cdrInfos;

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            _logger.LogInformation($"Inserting batch of {batchToInsert.Count} records for file {cdrFile.CDRFileName}...");

            //await mediator.Send(new CreateCDRInfoRangeCommand { cDRInfoResponse = batchToInsert });
            await new CDRProcessor(connectionStr, _logger).ProcessBatchCDRsAsync(batchToInsert);

            _logger.LogInformation($"Batch of {batchToInsert.Count} records inserted in {stopwatch.ElapsedMilliseconds} ms for file {cdrFile.CDRFileName}");
            stopwatch.Stop();
        }
        private void MoveFileToProcessedFolder(CDRFileToProcessResponse cdrFile)
        {
            var processedPath = Path.Combine(Path.GetDirectoryName(cdrFile?.ExistencePath)!, "processed");
            if (!Directory.Exists(processedPath))
            {
                Directory.CreateDirectory(processedPath);
            }

            // Define the destination file path
            var destinationFilePath = Path.Combine(processedPath, cdrFile?.CDRFileName!);

            // If the file already exists, delete it
            if (File.Exists(destinationFilePath))
            {
                File.Delete(destinationFilePath);
                _logger.LogInformation($"File \"{destinationFilePath}\" already exists. Overwriting.");
            }

            File.Move(cdrFile?.ExistencePath!, Path.Combine(processedPath, cdrFile?.CDRFileName!));
            _logger.LogInformation($"Move CDR \"{cdrFile.CDRFileName}\" to {processedPath}");

            _logger.LogInformation($"Finish Working on CDR {cdrFile?.CDRFileName} at: {DateTimeOffset.Now}");

        }
        private void MoveFileToFailedFolder(CDRFileToProcessResponse cdrFile)
        {
            var processedPath = Path.Combine(Path.GetDirectoryName(cdrFile?.ExistencePath)!, "Failed");
            if (!Directory.Exists(processedPath))
            {
                Directory.CreateDirectory(processedPath);
            }
            // Define the destination file path
            var destinationFilePath = Path.Combine(processedPath, cdrFile?.CDRFileName!);

            // If the file already exists, delete it
            if (File.Exists(destinationFilePath))
            {
                File.Delete(destinationFilePath);
                _logger.LogInformation($"File \"{destinationFilePath}\" already exists. Overwriting.");
            }

            File.Move(cdrFile?.ExistencePath!, Path.Combine(processedPath, cdrFile?.CDRFileName!));
            _logger.LogInformation($"Move CDR \"{cdrFile.CDRFileName}\" to {processedPath}");

            _logger.LogInformation($"Finish Working on CDR {cdrFile?.CDRFileName} at: {DateTimeOffset.Now}");

        }
        #endregion

        #region Process Lines

        private async Task<List<CDRInfoResponse>> ProcessLinesAsync(string line, CDRFileToProcessResponse cdrFile, List<CDRAttributeResponse> cdrAttributes, List<UserAttributeVm> userAttributeVms, EventIdAndName eventRechageAndRecurring, ConcurrentDictionary<string, bool> processedCdrIds,IMediator mediator)
        {
            try
            {
                int applicationId = int.Parse(_configuration["AppSettings:RedBullAppId"]);
                var cdrInfos = new List<CDRInfoResponse>();
                var values = line.Split('|');
                var uniqueIdentifier = Guid.NewGuid();

                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();

                if (cdrFile.CDRTypeId <= 5)
                {
                    // Extract CDR ID 
                    string? cdrId = values[0]?.Trim();

                    // Skip if CDR ID is already processed
                    if (!string.IsNullOrEmpty(cdrId))
                    {
                        if (!processedCdrIds.TryAdd(cdrId, true)) return cdrInfos; // Already processed, skip
                    }
                }
                // Fetch contacts based on CDRTypeId
                List<ContactResponse> contacts = await GetContactIdandEnterbriseId(values, cdrFile.CDRTypeId,mediator);
                if (contacts == null || !contacts.Any()) return cdrInfos; // Early exit if no contacts found.

                // Determine the event name
                EventIdAndName eventIdAndName = (cdrFile.CDRTypeId is 1 or 2 or 3)
                    ? await GetEventIdAndName(cdrFile.CDRTypeId, values)
                    : eventRechageAndRecurring;

                var maxUserAttributeId = 0;
                // Optimize user attribute max computation
                if (userAttributeVms.Any())
                {
                    maxUserAttributeId = userAttributeVms.Max(x => x.Id);

                }


                var cdrTasks = new List<Task<CDRInfoResponse>>();

                // Process each attribute for every contact
                stopwatch.Restart();
                // Process each attribute for every contact in parallel with isolated DI scopes
                foreach (var item in userAttributeVms)
                {
                    bool isRowFinished = (item.Id == maxUserAttributeId);
                    foreach (var contact in contacts)
                    {
                        cdrTasks.Add(Task.Run(async () =>
                        {
                            using var scope = _scopeFactory.CreateScope();
                            var scopedMediator = scope.ServiceProvider.GetRequiredService<IMediator>();
                            return await CreateCDRInfo(values, cdrFile, cdrAttributes, item, contact.Id, uniqueIdentifier, scopedMediator, eventIdAndName, isRowFinished);
                        }));
                    }
                }


                var cdrResults = await Task.WhenAll(cdrTasks);
                _logger.LogInformation($"Finish CreateCDRInfo take {stopwatch.ElapsedMilliseconds} ms");

                stopwatch.Restart();
                var validCdrInfos = cdrResults.Where(cdr => cdr != null).ToList();
                cdrInfos.AddRange(validCdrInfos);

                // Process event history in parallel
                var eventTasks = new List<Task>();

                foreach (var cdr in validCdrInfos.Where(cdr => !string.IsNullOrEmpty(cdr.EventName)))
                {
                    eventTasks.Add(Task.Run(async () =>
                    {
                        using var scope = _scopeFactory.CreateScope(); // Creates a new DI scope for each task
                        var scopedMediator = scope.ServiceProvider.GetRequiredService<IMediator>();

                        var eventHistory = await AddToEventHistory(scopedMediator, cdr.EventId, "", "", cdr.ContactId, "", 0, 0, 0, 0, cdr.TotalAmount, cdr.CreationDate);
                        if (eventHistory != null && !string.IsNullOrEmpty(contacts.FirstOrDefault()?.EnterpriseUserId))
                        {
                            await GetAndCallWorkFlow(scopedMediator, cdr.EventName, contacts.First().EnterpriseUserId, "", applicationId.ToString(), "", "", "", "", eventHistory.Id);
                        }
                    }));
                }

                await Task.WhenAll(eventTasks);

                _logger.LogInformation($"Finish AddToEventHistory take {stopwatch.ElapsedMilliseconds} ms");

                return cdrInfos;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error processing line for file ID: {cdrFile.Id}");
                throw;
            }
        }
        private async Task<List<ContactResponse>> GetContactIdandEnterbriseId(string[] values, int CDRTypeId,IMediator mediator)
        {
            try
            {
                System.Diagnostics.Stopwatch sw = System.Diagnostics.Stopwatch.StartNew();
                int applicationId = int.Parse(_configuration["AppSettings:RedBullAppId"].ToString());

                string firstName = string.Empty, lastName = string.Empty, contactNumber = string.Empty, custId = string.Empty, subId = string.Empty, offerId = string.Empty;

                switch (CDRTypeId)
                {
                    case 1: //voice
                    case 2: //sms
                    case 3: //data
                        subId = values[1];
                        contactNumber = values[21];
                        custId = values[19];
                        break;
                    case 4: //Recharge
                        contactNumber = values[5];
                        subId = values[4];
                        break;
                    case 5: //Recurring
                        subId = values[1];
                        contactNumber = values[21];
                        custId = values[19];
                        offerId = values[364];
                        break;
                    case 6: //customerInfAll
                        custId = values[0];
                        firstName = values[35];
                        lastName = values[37];
                        break;
                    case 7: //infAcct
                        custId = values[1];
                        break;
                    case 8: //SubscriberAll
                        subId = values[0];
                        custId = values[1];
                        contactNumber = values[7];
                        break;
                    case 9: //infOffers
                        custId = values[2];
                        break;
                    case 10:
                        offerId = values[0];
                        break;
                    case 11: //hisOffers
                        subId = values[3];
                        custId = values[2];
                        break;
                    case 13: //SubscriberDump
                        contactNumber = values[1];
                        subId = values[9];
                        break;
                    case 15: //Pee_Free_Unit_Dump
                        custId = values[1];
                        break;
                }

                contactNumber = NormalizePhoneNumber(contactNumber);
 
                var contacts = await mediator.Send(new CheckContactExistByPhoneNumberQuery
                {
                    ContactNumber = contactNumber,
                    SubId = subId,
                    CustId = custId,
                    FirstName = firstName,
                    LastName = lastName,
                    OfferId = offerId,
                    AppId = applicationId,
                    CDRTypeId = CDRTypeId
                });
                _logger.LogInformation($"Get {contacts?.Count ?? 0} Contacts Take {sw.ElapsedMilliseconds}");
                sw.Stop();
                return contacts;

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing contact number for CDRTypeId {CDRTypeId}", CDRTypeId);
                throw;
            }
        }
        private async Task<CDRInfoResponse> CreateCDRInfo(string[] values, CDRFileToProcessResponse cdrFile, List<CDRAttributeResponse> cdrAttributes, UserAttributeVm userAttributeVm, int contactId, Guid uniqueIdentifier, IMediator mediator, EventIdAndName eventIdAndName, bool IsRowFinished)
        {
            try
            {
                int applicationId = int.Parse(_configuration["AppSettings:RedBullAppId"].ToString());

                var cdrAttribute = cdrAttributes.FirstOrDefault(x => x.Id == userAttributeVm.CDRAttributeId);

                if (userAttributeVm.Id == 0)
                    return null;

                var cdrAttributeValue = values[cdrAttribute!.Index - 1]; // -1 because The List Separated Values started from 0 
                if (string.IsNullOrWhiteSpace(cdrAttributeValue)|| cdrAttributeValue.Length>50)
                    return null;

                #region convert Date
                // Regular expression to match the pattern yyyyMMddHHmmss
                var regex = new Regex(@"^\d{14}$");
                var regex2 = new Regex(@"^\d{15}$");
                if (regex.IsMatch(cdrAttributeValue))
                {
                    // Try to parse the date string
                    if (DateTime.TryParseExact(cdrAttributeValue, "yyyyMMddHHmmss", null, System.Globalization.DateTimeStyles.None, out DateTime dateTime))
                    {
                        // Convert to the desired format yyyy-MM-dd HH:mm:ss
                        cdrAttributeValue = dateTime.ToString("yyyy-MM-dd HH:mm:ss");
                    }
                }
                else if (regex2.IsMatch(cdrAttributeValue))
                {
                    if (cdrAttributeValue.Length >= 15)
                    {
                        // Remove the last three characters (milliseconds)
                        cdrAttributeValue = cdrAttributeValue.Substring(0, 12) + "00";
                        if (DateTime.TryParseExact(cdrAttributeValue, "yyyyMMddHHmmss", null, System.Globalization.DateTimeStyles.None, out DateTime dateTime))
                        {
                            // Convert to the desired format yyyy-MM-dd HH:mm:ss
                            cdrAttributeValue = dateTime.ToString("yyyy-MM-dd HH:mm:ss");
                        }
                    }
                }
                #endregion

                var cdrSubAttributeValue = await GetSubAttributeValue(cdrAttribute, cdrAttributeValue);
                var totalAmount = await GetTotalAmount(cdrAttribute, values);
                var creationDate = await GetRedBullCreationDate(cdrAttribute, values);
                Stopwatch stopwatch = Stopwatch.StartNew();
                var peeOfferId = await GetPeeOfferId(values,cdrFile.CDRTypeId,mediator);
                stopwatch.Stop();
                _logger.LogInformation($"get peeOfferId from RedisDB take {stopwatch.ElapsedMilliseconds} ms and offerId is {peeOfferId}");
                return new CDRInfoResponse
                {
                    CDRTypeId = cdrFile.CDRTypeId,
                    ContactId = contactId,
                    CDRAttributeId = cdrAttribute.Id,
                    ApplicationId = applicationId,
                    UserAttributeId = userAttributeVm.Id,
                    IndexNumber = cdrAttribute.Index,
                    CDRAttributeName = cdrAttribute.Name,
                    CDRAttributeValue = cdrAttributeValue,
                    CDRSubAttributeValue = cdrSubAttributeValue,
                    EventId = eventIdAndName.EventId,
                    FileId = cdrFile.Id,
                    RowUniqueIdentifier = uniqueIdentifier,
                    CreationDate = creationDate,
                    EventName = eventIdAndName.EventName,
                    TotalAmount = totalAmount,
                    IsRowFinished = IsRowFinished,
                    PeeOfferId = peeOfferId
                };


            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing User attribute: {attributeId}", userAttributeVm.Id);
                return null;
            }
        }
        private async Task<EventIdAndName> GetEventIdAndName(int CDRTypeId, string[] values)
        {
            try
            {
                Stopwatch stopwatch = new();
                stopwatch.Start();
                if (CDRTypeId == 1 ||
                    CDRTypeId == 2 ||
                    CDRTypeId == 3)
                {
                    var freeUnitId = !string.IsNullOrWhiteSpace(values[123]?.Trim()) ? values[123].Trim()
                                         : !string.IsNullOrWhiteSpace(values[131]?.Trim()) ? values[131].Trim() : null;
                    if (freeUnitId != null)
                    {
                        var serviceName = await GetBundleServiceCachedAsync(freeUnitId);
                        //_logger.LogInformation($"get service For CDRTypeId : {CDRTypeId} at {stopwatch.ElapsedMilliseconds} ms");
                        stopwatch.Stop();
                        if (serviceName != null)
                        {
                            return await GetEventIdAndNameCachedAsync (serviceName);
                        }
                        else
                        {
                            _logger.LogWarning($" service is not found For freeUnitId :{freeUnitId} CDRTypeId : {CDRTypeId}");
                        }
                    }
                    else
                    {
                        //_logger.LogInformation(" FreeUnitId is Null For CDRTypeId : {CDRTypeId}", CDRTypeId);
                        //var usageServiceType = !string.IsNullOrWhiteSpace(values[24]?.Trim()) ? values[24].Trim() : null;
                        //var eventService = await mediator.Send(new GetEventByNameQuery { Name = "" });

                        //if (eventService?.Name != null)
                        //{
                        //    return eventService != null ? new EventIdAndName(eventService.Id, eventService.Name) : new EventIdAndName(0, "");
                        //}
                        //else
                        //{
                        //    _logger.LogWarning($" eventService is not found for usageServiceType :{usageServiceType} CDRTypeId : {CDRTypeId}");
                        //}
                    }
                }
                stopwatch.Stop();
                return new EventIdAndName(0, "");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing event for CDRTypeId : {CDRTypeId}", CDRTypeId);
                return new EventIdAndName(0, "");
            }
        }
        private async Task<EventIdAndName> GetEventIdAndNameForRechargeAndRecurring(int CDRTypeId, IMediator mediator)
        {
            try
            {
                Stopwatch stopwatch = new();
                stopwatch.Start();
                if (CDRTypeId == 4 || CDRTypeId == 13)
                {
                    var eventService = await mediator.Send(new GetEventByNameQuery { Name = "Recharge" });
                    //_logger.LogInformation($"Get Recharge event Service Take {stopwatch.ElapsedMilliseconds}");
                    return eventService != null ? new EventIdAndName(eventService.Id, eventService.Name) : new EventIdAndName(0, "");
                }
                if (CDRTypeId == 5)
                {
                    stopwatch.Restart();
                    var eventService = await mediator.Send(new GetEventByNameQuery { Name = "Recurring" });
                    //_logger.LogInformation($"Get Recharge event Service Take {stopwatch.ElapsedMilliseconds}");
                    return eventService != null ? new EventIdAndName(eventService.Id, eventService.Name) : new EventIdAndName(0, "");
                }

                return new EventIdAndName(0, "");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing event for CDRTypeId : {CDRTypeId}", CDRTypeId);
                return new EventIdAndName(0, "");
            }
        }
        private async Task<decimal?> GetTotalAmount(CDRAttributeResponse cdrAttribute, string[] values)
        {
            try
            {
                if (cdrAttribute.CDRTypeId == 4 || cdrAttribute.CDRTypeId == 13)
                {
                    if (decimal.TryParse(values[2], out decimal result))
                    {
                        return result;
                    }
                    else
                    {
                        return 0m;
                    }
                }
                else if ((cdrAttribute.CDRTypeId == 1 && cdrAttribute.Index == 41) ||
                    (cdrAttribute.CDRTypeId == 2 && cdrAttribute.Index == 41) ||
                    (cdrAttribute.CDRTypeId == 3 && cdrAttribute.Index == 41) ||
                    (cdrAttribute.CDRTypeId == 5 && cdrAttribute.Index == 41))
                {
                    if (decimal.TryParse(values[40], out decimal result))
                    {
                        return result;
                    }
                    else
                    {
                        return 0m;
                    }
                }
                return 0m;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing totalamount for attribute ID: {attributeId}", cdrAttribute.Id);
                return 0m;
            }
        }
        private async Task<string> GetSubAttributeValue(CDRAttributeResponse cdrAttribute, string value)
        {
            try
            {
                if (cdrAttribute.HasSubAttribute)
                {
                    return cdrAttribute.HasSubAttribute
                  ? await GetSubAttributeValueCachedAsync(cdrAttribute, value)
                  : string.Empty;
                }
                return string.Empty;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting sub-attribute value for attribute ID: {attributeId}, value: {value}", cdrAttribute.Id, value);
                return "Unknown";
            }
        }
        private async Task<string> GetPeeOfferId(string [] values,int CDRTypeId, IMediator mediator)
        {
            try
            {
                string freeUnitId1 = string.Empty, freeUnitTypeId1 = string.Empty, freeUnitId2 = string.Empty, freeUnitTypeId2 = string.Empty, freeUnitId=string.Empty, freeUnitTypeId=string.Empty;
                switch (CDRTypeId)
                {
                    case 1: //voice
                    case 2: //sms
                    case 3: //data
                        freeUnitId1 = values[123];
                        freeUnitId2 = values[131];
                        freeUnitTypeId1 = values[124];
                        freeUnitTypeId2 = values[132];
                        break;
                }
                //return await GetPeeOfferIdAsync(freeUnitId1, freeUnitTypeId1, freeUnitId2, freeUnitTypeId2, mediator);
                if (!string.IsNullOrEmpty(freeUnitId1) && !string.IsNullOrEmpty(freeUnitTypeId1)) {
                    freeUnitId = freeUnitId1;
                    freeUnitTypeId = freeUnitTypeId1;
                }else if (!string.IsNullOrEmpty(freeUnitId2) && !string.IsNullOrEmpty(freeUnitTypeId2))
                {
                    freeUnitId = freeUnitId2;
                    freeUnitTypeId = freeUnitTypeId2;
                }
                else
                {
                    return string.Empty;
                }
                //return  await mediator.Send(new GetOfferIdByFreeUnitQuery() { freeUnitId = freeUnitId, freeUnitTypeId = freeUnitTypeId });

                return await GetAllPeeFreeUnitsCachedAsync( freeUnitId , freeUnitTypeId);
            
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        private async Task<DateTime> GetRedBullCreationDate(CDRAttributeResponse cdrAttribute, string[] values)
        {
            try
            {
                var creationDate = DateTime.Now;

                switch (cdrAttribute.CDRTypeId)
                {
                    case 1:
                    case 2:
                    case 3:
                    case 5:
                        creationDate = ParseDateString(values[10]);
                        break;
                    case 6:
                        creationDate = ParseDateString(values[12]);
                        break;
                    case 4:
                        creationDate = ParseDateString(values[29]);
                        break;
                    case 8:
                        creationDate = ParseDateString(values[20]);
                        break;
                    case 7:
                        creationDate = ParseDateString(values[15]);
                        break;
                    case 9:
                        creationDate = ParseDateString(values[13]);
                        break;
                    case 10:
                        creationDate = ParseDateString(values[29]);
                        break;
                    case 11:
                        creationDate = ParseDateString(values[13]);
                        break;
                    case 13:
                        creationDate = ParseDateString(values[0]);
                        break;
                    case 15:
                        creationDate = ParseDateString(values[22]);
                        break;
                }

                return creationDate;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error on get Creation Date for attribute ID: {attributeId}", cdrAttribute.Id);
                return DateTime.Now;
            }
        }
        static DateTime ParseDateString(string dateString)
        {
            if (dateString.Length >= 15)
            {
                // Remove the last three characters (milliseconds)
                dateString = dateString.Substring(0, 12) + "00";
            }
            string[] formats = { "yyyyMMdd", "yyyyMMddHHmmss" };
            DateTime dateTime = DateTime.ParseExact(dateString, formats, CultureInfo.InvariantCulture, DateTimeStyles.None);
            return dateTime;
            //DateTime dateTime = DateTime.ParseExact(dateString, "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
            //return dateTime;
        }
        #endregion

        #region FireWorkFlow
        private async Task GetAndCallWorkFlow(IMediator _mediator, string eventName, string customerExternalId, string authUsername, string authAppId, string productIds, string orderId, string checkoutId, string fullfillmentId, int EventHistoryId)
        {
            ApplicationEventResponse eventResponse = _mediator.Send(new GetAllApplicationEventsQuery() { ApplicationId = int.Parse(authAppId), Username = authUsername, FilterByWorkflowDef = true })
                .Result.FirstOrDefault(x => x.Name == eventName);
            if (eventResponse != null)
            {
                int eventId = eventResponse.Id;
                List<ApplicationWorkflowResponse> applicationWorkflowResponses = await _mediator.Send(new GetAllApplicationWorkflowsQuery()
                {
                    ApplicationId = int.Parse(authAppId),
                    Username = authUsername
                });

                if (applicationWorkflowResponses != null)
                {
                    if (applicationWorkflowResponses.Any())
                    {
                        Task.Run(() =>
                        {
                            foreach (ApplicationWorkflowResponse item in applicationWorkflowResponses)
                            {
                                DataRoot dataRoot = JsonConvert.DeserializeObject<DataRoot>(item.Data);
                                if (dataRoot.activities.Any())
                                {
                                    ConnekioMarketingTool.Application.Model.Activity activityEvent = dataRoot.activities.FirstOrDefault(a => a.type == "EventReciever");
                                    if (activityEvent != null)
                                    {
                                        foreach (Property property in activityEvent.properties)
                                        {
                                            if (property.expressions.Literal == eventId.ToString())
                                            {
                                                WorkflowTrackData workflowTrackData = new WorkflowTrackData();
                                                workflowTrackData.ApplicationId = authAppId;
                                                workflowTrackData.EventName = eventName;
                                                workflowTrackData.EventId = eventId.ToString();
                                                workflowTrackData.ExternalUserId = customerExternalId;
                                                workflowTrackData.ProductIds = productIds;
                                                workflowTrackData.OrderId = orderId;
                                                workflowTrackData.EventHistoryId = EventHistoryId;
                                                workflowTrackData.CheckOutId = checkoutId;
                                                workflowTrackData.FullfillmentId = fullfillmentId;

                                                var responseStr = CallWorkFlowAPIAsync(item.DefinitionId, workflowTrackData);

                                                //    break;
                                            }
                                        }
                                    }

                                    ConnekioMarketingTool.Application.Model.Activity activityWaitForEvent = dataRoot.activities.FirstOrDefault(a => a.type == "WaitForEvent");
                                    if (activityWaitForEvent != null)
                                    {
                                        foreach (Property property in activityWaitForEvent.properties)
                                        {
                                            if (property.name == "Signal" && property.expressions.Literal == eventId.ToString())
                                            {
                                                WorkflowTrackData workflowTrackData = new WorkflowTrackData();
                                                workflowTrackData.ApplicationId = authAppId;
                                                workflowTrackData.EventName = eventName;
                                                workflowTrackData.EventId = eventId.ToString();
                                                workflowTrackData.ExternalUserId = customerExternalId;
                                                workflowTrackData.ProductIds = productIds;
                                                workflowTrackData.OrderId = orderId;
                                                workflowTrackData.EventHistoryId = EventHistoryId;
                                                workflowTrackData.CheckOutId = checkoutId;
                                                workflowTrackData.FullfillmentId = fullfillmentId;
                                                var responseStr = CallWorkFlowAPIAsync(item.DefinitionId, workflowTrackData);
                                                //  break;
                                            }
                                        }
                                    }
                                }
                            }
                        });
                    }
                }
                else
                {
                    _logger.LogError($"No WorkFlow found for event {eventName} with ID {eventId} and application Id {authAppId}");

                }
            }
            else
            {
                //_logger.LogInformation($"Event With Name {eventName} and customerExternalId {customerExternalId} and EventHistoryId {EventHistoryId} is not found for application Id {authAppId}");
            }
        }

        public async Task<string> CallWorkFlowAPIAsync(string workflowDefinitionId, WorkflowTrackData workflowTrackData)
        {
            try
            {
                // Create an instance of HttpClient
                using (HttpClient _httpClient = new HttpClient())
                {
                    _logger.LogInformation("Workflow Triggered");

                    var connekioMT_ElsaAPIUrl = _configuration.GetValue<string>("AppSettings:WorkFlowAPI");
                    if (_httpClient.BaseAddress == null)
                    {
                        _httpClient.BaseAddress = new Uri(connekioMT_ElsaAPIUrl);
                    }
                    //var workflowUrl = $"{connekioMT_ElsaAPIUrl}/{workflowDefinitionId}";
                    var workflowRelativeUrl = new Uri(workflowDefinitionId, UriKind.Relative); ;

                    //var workflowUri = new Uri(workflowRelativeUrl, UriKind.Absolute);
                    _logger.LogInformation(
                    $"*****************************************************************\n" +
                    $"*************************    URI   ******************************\n" +
                    $"*****************************************************************\n" +
                    $"{connekioMT_ElsaAPIUrl}/{workflowDefinitionId}"
                    );
                    var workflowTrackDataStr = System.Text.Json.JsonSerializer.Serialize(workflowTrackData);
                    var content = new StringContent(workflowTrackDataStr, Encoding.UTF8, "application/json");

                    var response = await _httpClient.PostAsync(workflowRelativeUrl, content);
                    if (response.IsSuccessStatusCode)
                        return await response.Content.ReadAsStringAsync();
                    else
                        return null!;
                }
            }
            catch (Exception e)
            {
                _logger.LogCritical($"Exception in CallWorkFlowAPIAsync: {JsonConvert.SerializeObject(workflowTrackData)}" +
                    $"\n workflowDefinitionId: {workflowDefinitionId}" +
                    $"\n exception:{JsonConvert.SerializeObject(e)}");
                return null!;
            }
        }


        private async Task<AccountContactEventHistoryResponse> AddToEventHistory(IMediator _mediator, int? eventid, string workFlowDefinitionId, string workFlowDefinitionVersionId
     , int contactId, string productId, int orderid, int shopifyFulfillmentResponseid, int checkoutId, int cartId, decimal? TotalAmount, DateTime? creationDateFromSupplier)
        {

            var eventHistory = await _mediator
                    .Send(new CreateAccountContactEventWithHistoryCommand
                    {
                        AccountContactEventHistoryResponse = new ConnekioMarketingTool.Application.Responses.AccountContactEventHistoryResponse
                        {
                            EventId = eventid.GetValueOrDefault(0),
                            WorkFlowDefinitionId = workFlowDefinitionId,
                            WorkFlowDefinitionVersionId = workFlowDefinitionVersionId,
                            ContactId = contactId,
                            ProductId = productId,
                            OrderId = orderid,
                            FullfillmentId = shopifyFulfillmentResponseid,
                            CheckOutId = checkoutId,
                            CartId = cartId,
                            TotalAmount = TotalAmount,
                            CreationDateFromSupplier = creationDateFromSupplier

                        }
                    });

            return eventHistory;

        }
        #endregion

        #region Extract Contacts Data
        private async Task ProcessContactsSalahAsync(CDRFileToProcessResponse cdrFile, string[] lines, IMediator mediator)
        {

            _logger.LogInformation($"ProcessContactsSalahAsync CDRTypeId: {cdrFile.CDRTypeId} and CDR File {cdrFile.CDRFileName} started !!");

            int applicationId = int.Parse(_configuration["AppSettings:RedBullAppId"]);
            int batchSize = int.Parse(_configuration["AppSettings:ContactBatchSize"] ?? "5000");
            string connectionStr = _configuration.GetConnectionString("DefaultConnection");
            var cdrContactsPhoneNumberList = new List<string>();
            var cdrContactsIdList = new List<string>();

            //var cdrContactsCommand = new List<CdrContact?>(batchSize);
            List<Contact> batch = new List<Contact>();
            List<Contact> batchUpdate = new List<Contact>();
            //var cdrContactsCommandToUpdate = new List<Contact?>(batchSize);

            int skippedLines = 0, updatedContacts = 0, insertedContacts = 0;
            System.Diagnostics.Stopwatch sw = System.Diagnostics.Stopwatch.StartNew();

            foreach (var line in lines)
            {

                // Validate line
                if (string.IsNullOrWhiteSpace(line))
                {
                    Interlocked.Increment(ref skippedLines);
                    _logger.LogInformation("Skipped empty or whitespace line");
                    continue;
                }

                var values = line.Split('|');

                if (values.Length <= 7)
                {
                    Interlocked.Increment(ref skippedLines);
                    _logger.LogWarning("Invalid line format");
                    continue;
                }

                switch (cdrFile.CDRTypeId)
                {
                    case 6:

                        Contact contactUpdate_obj = new Contact
                        {

                            EnterpriseUserId = values[0],
                            LastName = values[37].Trim(),
                            FirstName = values[35].Trim()
                        };
                        if (!cdrContactsIdList.Contains(contactUpdate_obj.EnterpriseUserId + "_" + contactUpdate_obj.FirstName + "_" + contactUpdate_obj.LastName))
                        {
                            cdrContactsIdList.Add(contactUpdate_obj.EnterpriseUserId + "_" + contactUpdate_obj.FirstName + "_" + contactUpdate_obj.LastName);
                            //cdrContactsCommand.Add(cdrContact);
                            batchUpdate.Add(contactUpdate_obj);
                        }
                        Interlocked.Increment(ref updatedContacts);
                        break;

                    case 8: // SubscriberAll
                        {
                            string phoneNumber = NormalizePhoneNumber(values[7]);
                            //CdrContact cdrContact = new CdrContact
                            //{
                            //    ApplicationId = applicationId,
                            //    Active = true,
                            //    RedBullSubId = values[0],
                            //    EnterpriseUserId = values[1],
                            //    PhoneNumber = phoneNumber
                            //};
                            Contact contact_obj = new Contact
                            {
                                ApplicationId = applicationId,
                                Active = true,
                                RedBullSubId = values[0],
                                EnterpriseUserId = values[1],
                                PhoneNumber = phoneNumber
                            };
                            if (!cdrContactsPhoneNumberList.Contains(contact_obj.PhoneNumber + "_" + contact_obj.RedBullSubId))
                            {
                                cdrContactsPhoneNumberList.Add(contact_obj.PhoneNumber + "_" + contact_obj.RedBullSubId);
                                //cdrContactsCommand.Add(cdrContact);
                                batch.Add(contact_obj);
                            }
                            Interlocked.Increment(ref insertedContacts);

                            break;
                        }
                    default:
                        Interlocked.Increment(ref skippedLines);
                        _logger.LogInformation($"Unsupported CDRTypeId: {cdrFile.CDRTypeId}");
                        break;
                }

                // Process batches
                if (batch.Count >= batchSize && cdrFile.CDRTypeId == 8)
                {
                    _logger.LogInformation($"ContactsProcessor CDRTypeId: {cdrFile.CDRTypeId} started Processing {batch} contacts");

                    await new ContactsProcessor(connectionStr, _logger).ProcessBatchSubscribersAsync(batch);

                    _logger.LogInformation($"ContactsProcessor CDRTypeId: {cdrFile.CDRTypeId} Finished Processing {batch} contacts");

                    batch = new List<Contact>();
                }
                else if (batchUpdate.Count >= batchSize && cdrFile.CDRTypeId == 6)
                {
                    //await BulkUpdateContactsAsync(cdrContactsCommandToUpdate, mediator);
                    //cdrContactsCommandToUpdate = new List<Contact?>();
                    _logger.LogInformation($"ContactsProcessor CDRTypeId: {cdrFile.CDRTypeId} started Processing {batch} contacts");

                    //await BulkInsertContactsAsync(cdrContactsCommand, mediator);
                    await new ContactsProcessor(connectionStr, _logger).ProcessBatchCustomersAsync(batchUpdate);

                    _logger.LogInformation($"ContactsProcessor CDRTypeId: {cdrFile.CDRTypeId} Finished Processing {batch} contacts");

                    //cdrContactsCommand = new List<CdrContact?>();
                    batchUpdate = new List<Contact>();
                }
            }


            // Final batch processing after all threads
            if (batch.Any())
            {
                if (cdrFile.CDRTypeId == 8)
                {
                    await new ContactsProcessor(connectionStr, _logger).ProcessBatchSubscribersAsync(batch);
                    //await BulkInsertContactsAsync(cdrContactsCommand, mediator);
                }
                else if (cdrFile.CDRTypeId == 6)
                {
                    await new ContactsProcessor(connectionStr, _logger).ProcessBatchCustomersAsync(batch);

                    //await BulkUpdateContactsAsync(cdrContactsCommandToUpdate, mediator);
                }
            }
            _logger.LogInformation($"ProcessContactsSalahAsync CDRTypeId: {cdrFile.CDRTypeId} and CDR File {cdrFile.CDRFileName} Finished !!");

            _logger.LogInformation($"All contacts have been processed successfully. Took {sw.ElapsedMilliseconds} ms.");
        }
        private string NormalizePhoneNumber(string phoneNumber)
        {
            if (!string.IsNullOrEmpty(phoneNumber) && !phoneNumber.StartsWith("966"))
                return "966" + phoneNumber;
            return phoneNumber;
        }

        #endregion

        #region Caching Helper Functions
        private async Task<List<CDRAttributeResponse>> GetCDRAttributesAsync(int cdrTypeId, IMediator mediator)
        {
            string cacheKey = $"CDRAttributes_{cdrTypeId}";
            if (!_memoryCache.TryGetValue(cacheKey, out List<CDRAttributeResponse>? cdrAttributes))
            {
                cdrAttributes = await mediator.Send(new GetAllCDRAttributesQuery { cDRTypeId = cdrTypeId });
                _memoryCache.Set(cacheKey, cdrAttributes, TimeSpan.FromDays(1)); // Cache for 1 Day
            }
            return cdrAttributes!;
        }
        private async Task<List<UserAttributeVm>> GetUserAttributesAsync(int cdrTypeId, IMediator mediator)
        {
            string cacheKey = $"UserAttributes_{cdrTypeId}";
            if (!_memoryCache.TryGetValue(cacheKey, out List<UserAttributeVm>? userAttributes))
            {
                userAttributes = await mediator.Send(new GetAllUserAttributesByTypeIdQuery { cdrTypeId = cdrTypeId });
                _memoryCache.Set(cacheKey, userAttributes, TimeSpan.FromHours(12));
            }
            return userAttributes!;
        }
        private async Task<string?> GetBundleServiceCachedAsync(string freeUnitId)
        {
            const string cacheKey = "All_BundleServices";
            if (string.IsNullOrEmpty(freeUnitId))
                return "Unknown";
            if (!_memoryCache.TryGetValue(cacheKey, out List<BundleServiceResponse>? allservices))
            {
                using var scope = _scopeFactory.CreateScope(); // Creates a new DI scope for each task
                var scopedMediator = scope.ServiceProvider.GetRequiredService<IMediator>();
                allservices = await scopedMediator.Send(new GetAllBundleServicesQuery {});
                _memoryCache.Set(cacheKey, allservices , TimeSpan.FromHours(1));
            }

            return allservices.FirstOrDefault(s => s.FreeUnitId == freeUnitId)?.Name ?? "Unknown";
        }
        private async Task<EventIdAndName> GetEventIdAndNameCachedAsync(string eventName)
        {
            string cacheKey = $"All_Events";
            int applicationId = int.Parse(_configuration["AppSettings:RedBullAppId"]);

            if (string.IsNullOrEmpty(eventName))
                return null;
            if (!_memoryCache.TryGetValue(cacheKey, out List<ApplicationEventResponse>? alleventIdAndName))
            {
                using var scope = _scopeFactory.CreateScope(); // Creates a new DI scope for each task
                var scopedMediator = scope.ServiceProvider.GetRequiredService<IMediator>();
                alleventIdAndName = await scopedMediator.Send(new GetAllEventsQuery { AppId = applicationId });
                _memoryCache.Set(cacheKey, alleventIdAndName, TimeSpan.FromHours(23));
            }
            var match = alleventIdAndName.FirstOrDefault(e => e.Name == eventName);
            return match != null ? new EventIdAndName(match.Id, match.Name) : new EventIdAndName(0, "");

        }
        private async Task<string> GetSubAttributeValueCachedAsync(CDRAttributeResponse cdrAttribute, string value)
        {
            string cacheKey = $"SubAttribute_{cdrAttribute.Id}_{value}";

            if (!_memoryCache.TryGetValue(cacheKey, out string? subAttributeValue))
            {
                using var scope = _scopeFactory.CreateScope(); // Creates a new DI scope for each task
                var scopedMediator = scope.ServiceProvider.GetRequiredService<IMediator>();
                subAttributeValue = await scopedMediator.Send(new GetSubAttributeNameByAttributeIdAndValueQuery { AttributeId = cdrAttribute.Id, Value = value });
                _memoryCache.Set(cacheKey, subAttributeValue ?? "Unknown", TimeSpan.FromHours(12));
            }

            return subAttributeValue!;
        }
        private async Task<string> GetAllPeeFreeUnitsCachedAsync(string freeUnitId, string freeUnitTypeId)
        {
            var data = await _redisCache.GetPeeFreeUnitDataAsync( freeUnitId + freeUnitTypeId);
            //if (string.IsNullOrEmpty(data))
            //{
            //    using var scope = _scopeFactory.CreateScope();
            //    var scopedMediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            //    data = await scopedMediator.Send(new GetOfferIdByFreeUnitQuery() { freeUnitId = freeUnitId, freeUnitTypeId = freeUnitTypeId });
            //    await _redisCache.CachePeeFreeUnitDataAsync("PeeFreeUnitData" + "_" + freeUnitId + "_" + freeUnitTypeId, data);
            //}
            return data!;

        }
        private async Task GetAllPeeFreeUnitCachedAsync()
        {
            using var scope = _scopeFactory.CreateScope();
            var scopedMediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            Stopwatch stopwatch = Stopwatch.StartNew(); 
            var allPees = await scopedMediator.Send(new GetAllOfferIdByFreeUnitQuery() {});
            await Parallel.ForEachAsync(allPees, async (pee, token) =>
            {
                var data = await _redisCache.GetPeeFreeUnitDataAsync( pee.FREE_UNIT_ID + pee.FREE_UNIT_TYPE_ID);
                if (string.IsNullOrEmpty(data))
                {
                    await _redisCache.CachePeeFreeUnitDataAsync( pee.FREE_UNIT_ID + pee.FREE_UNIT_TYPE_ID, pee.OFFERING_ID);
                }
            });
            stopwatch.Stop();
            _logger.LogWarning($"GetAllPeeFreeUnitCachedAsync Has been finished and it takes {stopwatch.ElapsedMilliseconds} ms");
        }

        #endregion

        #region PeeFreeUnit Logic Functions
        private async Task ProcessPeeFreeUnitFilesAsync(CDRFileToProcessResponse cdrFile, IMediator mediator)
        {

            if (!FileLock.LockFile(cdrFile.ExistencePath))
            {
                _logger.LogWarning("File is already being processed: {file}", cdrFile.CDRFileName);
                return;
            }

            if (!File.Exists(cdrFile.ExistencePath))
            {
                _logger.LogWarning("File does not exist or has already been processed: {file}", cdrFile.CDRFileName);
                return;
            }
            int counter = 0;
            int batchSize = int.Parse(_configuration["AppSettings:CDRInfoBatchSize"] ?? "10000"); // Batch size for inserting records
            ConcurrentBag<PeeFreeUnitDataResponse> validPeeFreeUnitIds = new ConcurrentBag<PeeFreeUnitDataResponse>(); // Thread-safe collection for batch processing
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var totalStopwatch = System.Diagnostics.Stopwatch.StartNew(); // Track total processing time
            var ReadDate = DateTime.Now;
            try
            {
                stopwatch.Restart();
                await mediator.Send(new UpdateCDRFileStatusCommand
                {
                    FileId = cdrFile.Id,
                    IsProcessing = true,
                    ProcessingResponse = ProcessingResponseEnum.ProcessStarted,
                    ReadDate = ReadDate,
                });
                _logger.LogInformation(message: $"Update CDRFile Status completed in {stopwatch.ElapsedMilliseconds} ms for file {cdrFile.CDRFileName}");

                var lines = File.ReadAllLines(cdrFile.ExistencePath, Encoding.UTF8);
                // Process Lines in Parallel

                await Parallel.ForEachAsync(lines, async (line, token) =>
                {

                    Interlocked.Increment(ref counter);
                    //_logger.LogInformation($"Read Line Number {counter} in file {cdrFile.CDRFileName}");
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        _logger.LogInformation($"Skipped empty line {counter} in file {cdrFile.CDRFileName}");
                        return;
                    }
                    var peeFreeUnitOpj = ProcessPeeFreeUnitLine(line);
                    if (peeFreeUnitOpj != null)
                        validPeeFreeUnitIds.Add(peeFreeUnitOpj);

                });

                // Final batch insertion
                if (validPeeFreeUnitIds.Any())
                {
                    var batches = SplitIntoBatches(validPeeFreeUnitIds.Distinct().ToList(), batchSize);
                    if (batches == null)
                    {
                        _logger.LogInformation($"No valid CdrInfos to insert in Batches for file {cdrFile.CDRFileName}");
                        return;
                    }
                    foreach (var batch in batches)
                    {
                        await InsertPeeFreeUnitBatchSafelyAsync(batch, cdrFile, mediator);

                    }
                    _logger.LogInformation($"Insert Batchs PeeFreeUnit take {stopwatch.ElapsedMilliseconds} ms ");

                }

                await mediator.Send(new UpdateCDRFileStatusCommand
                {
                    FileId = cdrFile.Id,
                    IsProcessing = false,
                    ProcessingResponse = ProcessingResponseEnum.Success,
                    FinishProcessingDate = DateTime.Now,
                    ReadDate = ReadDate,
                    NumberOfInsertedAttributes = lines.Length,
                    ProcessingTime = (DateTime.Now - ReadDate).TotalMinutes.ToString("F2"),
                    LastInsertedRow = lines.Length,
                    IsSucceeded = true
                });

                MoveFileToProcessedFolder(cdrFile);
                _logger.LogInformation($"Finished processing file {cdrFile.CDRFileName} in {totalStopwatch.ElapsedMilliseconds} ms");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error processing file {cdrFile.CDRFileName}");
                await mediator.Send(new UpdateCDRFileStatusCommand
                {
                    FileId = cdrFile.Id,
                    IsProcessing = false,
                    ProcessingResponse = ProcessingResponseEnum.Failure,
                    ExceptionMessage = ex.Message,
                    ReadDate=ReadDate,
                    FinishProcessingDate = DateTime.Now,
                    ProcessingTime = (DateTime.Now - ReadDate).TotalMinutes.ToString("F2"),
                    LastInsertedRow = counter,
                    IsSucceeded = false
                });
                MoveFileToFailedFolder(cdrFile);
            }
            finally
            {
                FileLock.UnlockFile(cdrFile.ExistencePath);
            }
        }

        private PeeFreeUnitDataResponse ProcessPeeFreeUnitLine(string line)
        {
            try
            {
                var values = line.Split('|');
                if (values.Length == 0 || string.IsNullOrEmpty(values[0]) || values[0].Length > 25)
                {
                    return null;
                }
                return new PeeFreeUnitDataResponse
                {
                    CREATE_DATE = ParseDateString(values[22]),
                    FREE_UNIT_ID = values[0],
                    CUST_ID = values[1],
                    FREE_UNIT_TYPE_ID = values[6],
                    OFFERING_ID = values[31]
                };
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        private async Task InsertPeeFreeUnitBatchSafelyAsync(List<PeeFreeUnitDataResponse> PeeFreeUnitData, CDRFileToProcessResponse cdrFile, IMediator mediator)
        {
            var batchToInsert = PeeFreeUnitData;

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            _logger.LogInformation($"Inserting batch of {batchToInsert.Count} records for file {cdrFile.CDRFileName}...");

            await mediator.Send(new CreatePeeFreeUnitRangeCommand { peeFreeUnitDataResponse = batchToInsert });

            _logger.LogInformation($"Batch of {batchToInsert.Count} records inserted in {stopwatch.ElapsedMilliseconds} ms for file {cdrFile.CDRFileName}");
            stopwatch.Stop();
        }


        #endregion

        public record EventIdAndName(int EventId, string EventName);

    }
}
