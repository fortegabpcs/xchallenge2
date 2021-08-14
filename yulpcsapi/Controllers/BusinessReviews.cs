using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using yulpcsapi.Models;
using yulpcsapi.Repositories;

namespace yulpcsapi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BusinessReviewsController : ControllerBase
    {

        private readonly IConfiguration _config;
        private readonly ILogger<Controller> _logger;
        private readonly IBusinessReviewRepository _repository;
        static HttpClient client = new HttpClient();
        public BusinessReviewsController(ILogger<Controller> logger, IBusinessReviewRepository repository, IConfiguration config)
        {
            _logger = logger;
            _repository = repository;
            _config = config;
        }

        [HttpGet]
        public async Task<ActionResult<BusinessReview>> Get(int offset = 0, int pagesize = 10, string category = "", string location = "", int distance = 20)
        {
            Geolocation geolocation = null;
            if (!string.IsNullOrWhiteSpace(location))
            {
                geolocation = await GetGeocode(location);
            }

            var result = await _repository.GetBusinessReview(offset, pagesize, category, geolocation, distance);

            return Ok(result);
        }

        private async Task<Geolocation> GetGeocode(string locationQuery)
        {
            Geolocation geolocation = null;
            try
            {
                
                string mapEndpoint = _config.GetValue<string>("MAP_API_ENDPOINT");
                HttpResponseMessage response;
                var mapQuery = mapEndpoint + $"&query={locationQuery}";
                response = await client.GetAsync(mapQuery);
                if (response.IsSuccessStatusCode)
                {
                    geolocation = new Geolocation();
                    var locationString = await response.Content.ReadAsStringAsync();
                    var locationArray = JObject.Parse(locationString);
                    geolocation.Latitude = (decimal)locationArray["results"][0]["position"]["lat"];
                    geolocation.Longitude = (decimal)locationArray["results"][0]["position"]["lon"];
                }
            }
            catch (Exception e)
            {
                _logger.Log(LogLevel.Error, e.Message, e);
            }

            return geolocation;
        }
    }

}