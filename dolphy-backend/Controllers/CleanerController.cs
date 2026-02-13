using dolphy_backend.Implementations;
using dolphy_backend.Interfaces;
using Hangfire;
using Microsoft.AspNetCore.Mvc;

namespace dolphy_backend.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CleanerController() : ControllerBase
    {
        private readonly IFileCleaner _fileCleaner = new FileCleaner();

        [HttpPost("enqueue_clean")]
        public async Task<IActionResult> PostCleanCsv(IFormFile csvToClean, [FromBody] string instructionsJson)
        {
            string tempCsvPath = Path.Combine(Constants.storagePath, $"{Guid.NewGuid()}");
            string tempJsonPath = Path.Combine(Constants.storagePath, $"{Guid.NewGuid()}");
            string cleanedCsvPath = Path.Combine(Constants.storagePath, $"{Guid.NewGuid()}");

            try
            {
                using (var stream = new FileStream(tempCsvPath, FileMode.Create))
                {
                    await csvToClean.CopyToAsync(stream);
                }

                await System.IO.File.WriteAllTextAsync(tempJsonPath, instructionsJson);

                var processId = BackgroundJob.Enqueue<IFileCleaner>(cleaner =>
                    _fileCleaner.RunCsvCleaner(tempCsvPath, tempJsonPath, cleanedCsvPath));
                return Ok(new { message = "Job Completed", processId });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in cleaning the csv file: {ex.Message}");
                Response.Headers.Append("X-Processing-Error", "Clean Script Faiure: Returning original file.");
                return BadRequest("Job failed to complete successfully");
            }
        }
    }
}
