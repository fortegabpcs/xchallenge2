using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Newtonsoft.Json.Linq;

namespace yulpcsapi.Models
{
    public class BusinessReview
    {
        [Key]
        public int Id { get; set; }
        public string BusinessId { get; set; }
        public decimal Latitude { get; set; }
        public decimal Longitude { get; set; }
        public string Address { get; set; }
        //public decimal Distance { get; set; }
        public string BusinessJson { get; set; }
        public string ReviewsJson { get; set; }
        [NotMapped]
        public JObject Business { get; set; }
        [NotMapped]
        public JArray Reviews { get; set; }

    }
}