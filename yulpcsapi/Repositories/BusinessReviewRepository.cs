using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json.Linq;
using yulpcsapi.DataAccess;
using yulpcsapi.Models;

namespace yulpcsapi.Repositories
{
    public class BusinessReviewRepository : IBusinessReviewRepository
    {
        private readonly AppDBContext _context;
        public BusinessReviewRepository(AppDBContext context)
        {
            _context = context;

        }
        public async Task<List<BusinessReview>> GetBusinessReview(int offset = 0, int pagesize = 100, string category = "", Geolocation location = null, int distance = 20)
        {
            List<BusinessReview> businessReviews = new List<BusinessReview>();

            //var offsetParam = new SqlParameter("Offset", offset);
            //var pagesizeParam = new SqlParameter("Size", offset);

            string query = $"exec dbo.get_reviews_page @Offset={offset}, @Size={pagesize}, @distance={distance}";
            if(location != null)
                query += $", @lat ={location.Latitude}, @lon ={location.Longitude}";
            if(!String.IsNullOrWhiteSpace(category))
                query += $", @Category = {category}";
            

            var result = await _context.BusinessReviews.FromSqlRaw(query).ToListAsync();
             
              foreach(var row in result)
              {
                  Console.WriteLine(row.BusinessJson);
                  Console.WriteLine(row.ReviewsJson);
                  businessReviews.Add(new BusinessReview{
                      Id = row.Id,
                      BusinessId = row.BusinessId,
                      Latitude = row.Latitude,
                      Longitude = row.Longitude,
                      Address = row.Address,
                      //Distance = row.Distance,
                      Business = JObject.Parse(row.BusinessJson),
                      Reviews = JArray.Parse(row.ReviewsJson)
                  });
              }
            return businessReviews;
        }
    }
}