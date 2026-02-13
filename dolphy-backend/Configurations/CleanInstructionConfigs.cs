using System.Text.Json.Serialization;

namespace dolphy_backend.Configurations
{
    internal class CleanInstructionConfigs
    {
        [JsonPropertyName("datatype")]
        public required int DataType { get; set; }

        [JsonPropertyName("clean-method")]
        public required string[] CleanMethod {  get; set; }
    }
}
