using CDRMappingEngine;
using CDRMappingEngine.Redis;
using ConnekioMarketingTool.Application;
using ConnekioMarketingTool.Application.Model;
using ConnekioMarketingTool.Infrastructure;
using Microsoft.Extensions.Logging.EventLog;
using StackExchange.Redis;

var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

// Fetch values from appsettings.json
string serviceName = config["ServiceConfig:ServiceName"] ?? "RedBullCDRMappingEngine";
string logName = config["ServiceConfig:LogName"] ?? "RedBullCDRMappingEngine";
string sourceName = config["ServiceConfig:SourceName"] ?? "RedBullCDRMappingEngine";


IHost host = Host.CreateDefaultBuilder(args)
    .UseWindowsService(options =>
    {
        options.ServiceName = serviceName;
    })
    .ConfigureServices((context, services) =>
    {
        services.AddMemoryCache(); 
        services.AddHostedService<Worker>();
        services.AddInfrastructureServices(context.Configuration);
        services.AddApplicationServices(context.Configuration, AppDomain.CurrentDomain.GetAssemblies());
        services.AddSingleton<TrigerSegmentInWorkFlow>();
        services.AddSingleton<IConnectionMultiplexer>(ConnectionMultiplexer.Connect("localhost:6379"));
        services.AddScoped<IRedisCacheService, RedisCacheService>();

    })
    .ConfigureLogging((context, logging) =>
    {
        logging.ClearProviders();
        logging.AddConfiguration(
        context.Configuration.GetSection("Logging"));
        logging.AddEventLog(new EventLogSettings()
        {
            SourceName = sourceName,
            LogName = logName
        });
        logging.AddConsole();
    })
    .Build();
await host.RunAsync();
