using dolphy_backend.Interfaces;
using System.Diagnostics;

namespace dolphy_backend.Implementations
{
    public class FileCleaner : IFileCleaner
    {
        public async Task RunCsvCleaner(string csvPath, string instructionPath, string cleanedCsvPath) {
            try
            {
                var status = await RunCleanerProcess(csvPath, instructionPath, cleanedCsvPath);
                if (!status) throw new Exception("Failure in Cleaning the csv"); 
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in running the cleaner script: {ex.Message}");
            }
            finally {
                if (File.Exists(csvPath)) File.Delete(csvPath);
                if (File.Exists(instructionPath)) File.Delete(instructionPath);
            }

        }

        private async Task<bool> RunCleanerProcess(string csvPath, string instructionPath, string cleanedCsvPath) {
            string cleanerScriptPath = "../../cleanerscript.py";
            try
            {
                var processInfo = new ProcessStartInfo
                {
                    FileName = "python3",
                    Arguments = $"{cleanerScriptPath} {csvPath} {instructionPath} {cleanedCsvPath}",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                using (var process = new Process { StartInfo = processInfo })
                {
                    process.Start();

                    string output = await process.StandardOutput.ReadToEndAsync();
                    string error = await process.StandardError.ReadToEndAsync();

                    await process.WaitForExitAsync();

                    if (process.ExitCode != 0)
                    {
                        throw new Exception($"Python code failed to execute \n Error: {error}");
                    }

                    return true;
                }
            }
            catch (Exception ex) 
            {
                Console.WriteLine($"Cleaner Process Failure Exception: {ex.Message}");
            }
            return false;
        }
    }
}
