

using ConnekioMarketingTool.Application.Responses.OperatorsPlatform;
using ConnekioMarketingTool.Core.Entities.OperatorsPlatform;

namespace CDRMappingEngine.Redis
{
    public interface IRedisCacheService
    {
        Task CachePeeFreeUnitDataAsync(string cacheKey, string offeringId);
        Task<string> GetPeeFreeUnitDataAsync(string cacheKey);
    }
}
