using dolphy_backend.DataTypes;
using dolphy_backend.Implementations;
using dolphy_backend.Interfaces;
using Hangfire;
using Microsoft.AspNetCore.Mvc;

namespace dolphy_backend.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CleanerController : ControllerBase
    {
        private readonly IFileCleaner _fileCleaner;
        private readonly IFileHandler _fileHandler;

        public CleanerController(IFileCleaner fileCleaner, IFileHandler fileHandler)
        {
            _fileCleaner = fileCleaner;
            _fileHandler = fileHandler;
        }

        /// <summary>
        /// Endpoint to upload a csv - the csv is uploaded as batched to avoid overloading the in-memory of the browser
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        [HttpPost("uploadfile")]
        public async Task<IActionResult> UploadCsvBatch([FromForm] ChunkRequest request) 
        {
            try
            {
                if (request.CsvChunk != null && request.FileId != null) 
                {
                    var status = await _fileHandler.BatchUploadAsync(request.CsvChunk,
                                                            request.FileId,
                                                            request.ChunkIndex,
                                                            request.TotalChunks);
                    if (status != null && status["state"] == "file merged")
                    {
                        string fileId = status["FileId"] != null ? status["FileId"] : "0";
                        string filePath = status["FilePath"] != null ? status["FilePath"] : "";
                        return Ok(new { message = "File Uploaded", fileId , filePath});
                    }
                }
                return BadRequest("Batched File upload failed");
            }
            catch (Exception ex) 
            {
                Console.WriteLine($"File Batch upload failed due to : {ex.Message}");
                return BadRequest(ex.Message);
            }
        } 

        /// <summary>
        /// Endpoint to run the clean script of a csv
        /// </summary>
        /// <param name="csvToClean"></param>
        /// <param name="instructionsJson"></param>
        /// <returns></returns>
        [HttpPost("enqueue_clean")]
        public async Task<IActionResult> PostCleanCsv(IFormFile csvToClean, [FromBody] string instructionsJson)
        {
            var tempStorage = Path.Combine(Constants.homePath, Constants.inboundStorage);
            string tempCsvPath = Path.Combine(tempStorage, $"{Guid.NewGuid()}");
            string tempJsonPath = Path.Combine(tempStorage, $"{Guid.NewGuid()}");
            string cleanedCsvPath = Path.Combine(tempStorage, $"{Guid.NewGuid()}");

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

        /// <summary>
        /// Endpoint to get the rows of a CSV in pages format - pagination of a csv
        /// </summary>
        /// <param name="fileId"></param>
        /// <param name="pageNo"></param>
        /// <param name="pageSize"></param>
        /// <returns></returns>
        [HttpGet("csv_pages")]
        public async Task<IActionResult> GetCsvPages([FromQuery] string fileId, [FromQuery] int pageNo = 1, [FromQuery] int pageSize = 20) 
        {
            try 
            {
                var pageResult = await _fileHandler.GetCsvPaginationAsync(fileId, pageNo, pageSize);
                return Ok(new { message = "Pages ", pageResult });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Csv Pagination failed: {ex.Message}");
                return BadRequest("CSV Pagination failed to load");
            }
        }

    }
}
