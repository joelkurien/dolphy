namespace dolphy_backend.Interfaces
{
    internal interface IFileCleaner
    {
        public Task RunCsvCleaner(string csvPath, string instructionPath, string cleanedCsvPath);
    }
}
