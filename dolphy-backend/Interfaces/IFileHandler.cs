using dolphy_backend.DataTypes;

namespace dolphy_backend.Interfaces
{
    public interface IFileHandler
    {
        public Task<IDictionary<string, string>> BatchUploadAsync(IFormFile file, string file_id, int chunk_idx, int total_chunks);

        public Task<PageResult> GetCsvPaginationAsync(string file_id, int pageNo, int pageSize);
    }
}
