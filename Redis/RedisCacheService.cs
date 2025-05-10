using ConnekioMarketingTool.Application.Responses.OperatorsPlatform;
using ConnekioMarketingTool.Core.Entities.OperatorsPlatform;
using StackExchange.Redis;
using System.Diagnostics;
using System.Text.Json;

namespace CDRMappingEngine.Redis
{
    public class RedisCacheService : IRedisCacheService
    {
        private readonly IDatabase _cache;
        private readonly ILogger<RedisCacheService> _logger;

        public RedisCacheService(IConnectionMultiplexer redis, ILogger<RedisCacheService> logger)
        {
            _cache = redis.GetDatabase();
            _logger = logger;
        }

        public async Task CachePeeFreeUnitDataAsync(string cacheKey, string offeringId)
        {
            await _cache.StringSetAsync(cacheKey, offeringId);
        }

        public async Task<string> GetPeeFreeUnitDataAsync(string cacheKey)
        {
            var json = await _cache.StringGetAsync(cacheKey);
            return string.IsNullOrEmpty(json)
                ? string.Empty  : json;
        }


    }

}
