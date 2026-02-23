using dolphy_backend.DataTypes;
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
        private readonly IFileHandler _fileHandler = new FileHandler();

        [HttpPost("uploadfile")]
        public async Task<IActionResult> UploadCsvBatch([FromForm] ChunkRequest request) 
        {
            try
            {
                if (request.CsvChunk != null && request.FileId != null) 
                {
                    var status = await _fileHandler.BatchUpload(request.CsvChunk,
                                                            request.FileId,
                                                            request.ChunkIndex,
                                                            request.TotalChunks);
                    if (status != null && status["state"] == "file merged")
                    {
                        string fileId = status["FileId"] != null ? status["FileId"] : "0";
                        string filePath = status["FilePath"] != null ? status["FilePath"] : "";
                        return Ok(new { message = "File Uploaded", fileId , filePath});
                    }
                    Console.WriteLine(status);
                }
                return BadRequest("Batched File upload failed");
            }
            catch (Exception ex) 
            {
                Console.WriteLine($"File Batch upload failed due to : {ex.Message}");
                return BadRequest(ex.Message);
            }
        } 


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
