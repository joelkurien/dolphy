namespace dolphy_backend.DataTypes
{
    public class PageResult
    {
        public required List<IDictionary<string, string>> Data { get; set; }
        public int PageNo { get; set; }
        public int PageSize { get; set; }
        public int TotalPages { get; set; }
        public required string SourcedFrom { get; set; }
    }
}
