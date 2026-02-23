namespace dolphy_backend.Interfaces
{
    internal interface IFileHandler
    {
        public Task<IDictionary<string, string>> BatchUpload(IFormFile file, string file_id, int chunk_idx, int total_chunks);
    }
}
