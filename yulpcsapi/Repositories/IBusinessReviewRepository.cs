using System.Collections.Generic;
using System.Threading.Tasks;
using yulpcsapi.Models;

namespace yulpcsapi.Repositories
{
    public interface IBusinessReviewRepository
    {
         Task<List<BusinessReview>> GetBusinessReview(int offset, int pagesize, string category, Geolocation geolocation, int distance);
    }
}