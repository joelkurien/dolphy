using CsvHelper;
using dolphy_backend.DataTypes;
using dolphy_backend.Interfaces;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.IdentityModel.Tokens;
using StackExchange.Redis;
using System.Globalization;
using System.Text.Json;

namespace dolphy_backend.Implementations
{
    public class FileHandler : IFileHandler
    {
        private readonly IDistributedCache _redis;

        public FileHandler(IDistributedCache redis)
        {
            _redis = redis;
        }

        public async Task<IDictionary<string, string>> BatchUploadAsync(IFormFile? file, string file_id, int chunk_idx, int total_chunks) 
        {
            var tempPath = Path.Combine(Path.GetTempPath(), $"{file_id}");
            if(!Directory.Exists(tempPath)) { Directory.CreateDirectory(tempPath); }

            var chunkPath = Path.Combine(tempPath, $"chunk_{chunk_idx}");
            if (file != null)
            {
                using (var stream = new FileStream(chunkPath, FileMode.Create))
                {
                    await file.CopyToAsync(stream);
                }
            }

            if (Directory.GetFiles(tempPath).Length == total_chunks) 
            {
                var mergeData = await MergeBatchesAsync(tempPath, file_id);
                var filePath = mergeData[0];
                var totalRows = int.Parse(mergeData[1]);

                var metadata = new SessionFileMeta
                {
                    FileId = file_id,
                    FilePath = filePath,
                    TotalRows = totalRows
                };

                await _redis.SetStringAsync($"file:{file_id}:meta", 
                                            JsonSerializer.Serialize(metadata));

                return new Dictionary<string, string> {
                    { "state", "file merged" },
                    { "FileId", file_id },
                    { "FilePath", filePath }
                };
            }

            return new Dictionary<string, string>
            {
                { "state", "chunk uploaded" },
                { "ChunkId", chunk_idx.ToString() }
            };
        }

        public async Task<List<string>> MergeBatchesAsync(string tempPath, string file_id)
        {
            var fileStorageLoc = Path.Combine(Constants.homePath, Constants.inboundStorage);
            var finalPath = Path.Combine(fileStorageLoc, $"{file_id}.csv");

            using (var finalStream = new FileStream(finalPath, FileMode.Create)) 
            using (var writer = new StreamWriter(finalStream))
            {
                for(int i=0; i<Directory.GetFiles(tempPath).Length; i++)
                {
                    var chunkPath = Path.Combine(tempPath, $"chunk_{i}");
                    using(var chunkReader = new StreamReader(chunkPath))
                    {
                        string? line;
                        bool firstChunkLine = true;
                        while((line = await chunkReader.ReadLineAsync()) != null)
                        {
                            if(firstChunkLine && i > 0)
                            {
                                firstChunkLine = false;
                                continue;
                            }
                            await writer.WriteLineAsync(line);
                            firstChunkLine = false;
                        }
                    }
                    File.Delete(chunkPath);
                }
            }
            var totalRows = File.ReadLines(finalPath).Count();
            return [finalPath, totalRows.ToString()];
        }

        public async Task<PageResult> GetCsvPaginationAsync(string file_id, int pageNo, int pageSize) 
        {
            var result = new PageResult { Data = [], SourcedFrom = "Storage" };
            try
            {
                var fileMetaJson = await _redis.GetStringAsync($"file:{file_id}:meta");
                var fileMetaData = fileMetaJson != null ? JsonSerializer.Deserialize<SessionFileMeta>(fileMetaJson) : null;

                var filePath = fileMetaData != null ? fileMetaData.FilePath : "";

                var cacheKey = $"file:{file_id}:p{pageNo}:s{pageSize}";

                var cacheData = await _redis.GetStringAsync(cacheKey);
                if (cacheData != null)
                {
                    var cachedData = JsonSerializer.Deserialize<PageResult>(cacheData);
                    if (cachedData != null)
                    {
                        cachedData.SourcedFrom = "Cache";
                    }
                    return cachedData ?? new PageResult { Data = [], SourcedFrom = "Cache" };
                }

                var skipTo = (pageNo - 1) * pageSize;
                var pageRes = getCsvRows(filePath, skipTo, pageSize);

                int totalRows = fileMetaData != null ? fileMetaData.TotalRows : 0;

                var pageData = new PageResult
                {
                    Data = pageRes,
                    PageNo = pageNo,
                    PageSize = pageSize,
                    TotalPages = (int)Math.Ceiling((double)totalRows / pageSize),
                    SourcedFrom = "Storage"
                };

                var currentPageKey = await _redis.GetStringAsync($"file:{file_id}:current_page");
                if (currentPageKey != null)
                {
                    await _redis.RemoveAsync(currentPageKey);
                }

                await _redis.SetStringAsync(cacheKey, JsonSerializer.Serialize(pageData),
                    new DistributedCacheEntryOptions
                    {
                        AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(30)
                    });

                if (currentPageKey != null)
                    await _redis.SetStringAsync(currentPageKey, cacheKey);

                return pageData;
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Pagination Failed due to: {ex.Message}");
            }

            return result;
        }

        private List<IDictionary<string, string>> getCsvRows(string filePath, int skipTo, int pageSize)
        {
            var results = new List<IDictionary<string, string>>();

            try
            {
                using var reader = new StreamReader(filePath);
                using var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture);

                csvReader.Read();
                csvReader.ReadHeader();
                var headers = csvReader.HeaderRecord;

                if (headers == null) return results;

                for (int i = 0; i < skipTo; i++)
                {
                    if(!csvReader.Read()) break;
                }

                if (csvReader != null)
                {
                    var count = 0;
                    while (count < pageSize && csvReader.Read())
                    {
                        var row = new Dictionary<string, string>();
                        if (!headers.IsNullOrEmpty())
                        {
                            foreach (var header in headers ?? Array.Empty<string>())
                            {
                                row[header] = csvReader.GetField(header) ?? string.Empty;
                            }
                            count++;
                            results.Add(row);
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Failure to split the csv due to: {ex.Message}");
            }
            
            return results;
        }
    }
}
