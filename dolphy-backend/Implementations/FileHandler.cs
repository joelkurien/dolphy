using dolphy_backend.Interfaces;

namespace dolphy_backend.Implementations
{
    public class FileHandler : IFileHandler
    {
        public async Task<IDictionary<string, string>> BatchUpload(IFormFile? file, string file_id, int chunk_idx, int total_chunks) 
        {
            var tempPath = Path.Combine(Path.GetTempPath(), file_id);
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
                string finalPath = await MergeBatches(tempPath, file_id);
                return new Dictionary<string, string> {
                    { "state", "file merged" },
                    { "FileId", file_id },
                    { "FilePath", finalPath }
                };
            }

            return new Dictionary<string, string>
            {
                { "state", "chunk uploaded" },
                { "ChunkId", chunk_idx.ToString() }
            };
        }

        public async Task<string> MergeBatches(string tempPath, string file_id)
        {
            var finalPath = Path.Combine(Path.GetTempPath(), $"{file_id}.csv");

            using (var finalStream = new FileStream(finalPath, FileMode.Create)) 
            {
                for(int i=0; i<Directory.GetFiles(tempPath).Length; i++)
                {
                    var chunkPath = Path.Combine(tempPath, $"chunk_{i}");
                    using( var stream = new FileStream(chunkPath, FileMode.Open))
                    {
                        await stream.CopyToAsync(finalStream);
                    }
                    System.IO.File.Delete(chunkPath);
                }
            }
            return finalPath;
        }
    }
}
