namespace dolphy_backend.DataTypes
{
    public class SessionFileMeta
    {
        public Guid Id { get; set; } = Guid.NewGuid();
        public required string FileId { get; set; }
        public required string FilePath { get; set; }
        public int TotalRows { get; set; }
        public DateTime FileUploadTime { get; set; }
    }
}

