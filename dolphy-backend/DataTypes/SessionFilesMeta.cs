namespace dolphy_backend.DataTypes
{
    public class SessionFileMeta
    {
        public required string FileId { get; set; }
        public required string FilePath { get; set; }
        public int TotalRows { get; set; }
    }
}

