using dolphy_backend.DataTypes;
using Microsoft.EntityFrameworkCore;

namespace dolphy_backend.DBManagement
{
    public class DatabaseHandler : DbContext
    {
        public DatabaseHandler(DbContextOptions<DatabaseHandler> options) : base(options) { }

        public DbSet<SessionFileMeta> FileMetas { get; set; }
    }
}
