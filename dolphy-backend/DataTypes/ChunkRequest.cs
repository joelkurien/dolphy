namespace dolphy_backend.DataTypes
{
    public class ChunkRequest
    {
        public IFormFile? CsvChunk { get; set; }
        public string? FileId   { get; set; }
        public int ChunkIndex { get; set; }
        public int TotalChunks { get; set; }
    }
}
