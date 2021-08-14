using Microsoft.EntityFrameworkCore;
using yulpcsapi.Models;

namespace yulpcsapi.DataAccess
{
    public class AppDBContext : DbContext
    {
        public AppDBContext(DbContextOptions<AppDBContext> options)
             : base(options)
        {

        }
        public DbSet<BusinessReview> BusinessReviews { get; set; }
    }
}