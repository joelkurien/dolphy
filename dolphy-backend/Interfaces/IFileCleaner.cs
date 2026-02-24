using dolphy_backend.DataTypes;

namespace dolphy_backend.Interfaces
{
    public interface IFileCleaner
    {
        public Task RunCsvCleaner(string csvPath, string instructionPath, string cleanedCsvPath);
    }
}
