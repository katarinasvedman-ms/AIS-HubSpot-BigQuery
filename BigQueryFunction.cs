using Newtonsoft.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;
using Microsoft.Extensions.Configuration;
using System.Text;

namespace Company.Function
{
    public class BigQueryFunction
    {
        private readonly ILogger<BigQueryFunction> _logger;
        private readonly IConfiguration _configuration;
        private static BigQueryClient client;
        private readonly string projectId = "bigquery-test-441614";
        private bool account = true;

        public BigQueryFunction(ILogger<BigQueryFunction> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            try
            {
                GoogleCredential credential;
                string googleCloudKeyJson = _configuration["GoogleCloudKey"];
                if (string.IsNullOrEmpty(googleCloudKeyJson))
                {
                    _logger.LogError("GoogleCloudKey is not set in the configuration.");
                    throw new ArgumentNullException("GoogleCloudKey");
                }
                _logger.LogInformation($"app setting: {googleCloudKeyJson}");

                // Parse the JSON string to extract the key
                var googleCloudKey = JsonConvert.DeserializeObject<Dictionary<string, string>>(googleCloudKeyJson)["key"];
                _logger.LogInformation($"Extracted key: {googleCloudKey}");

                using (var credentialStream = new MemoryStream(Encoding.UTF8.GetBytes(googleCloudKey)))
                {
                    credential = GoogleCredential.FromStream(credentialStream);
                }
                client = BigQueryClient.Create(projectId, credential);
            }
            catch (System.Exception e)
            {
                _logger.LogInformation($"Error: {e.Message}, will not query BigQuery");
                account = false;
            }
        }

        [Function("BigQueryFunction")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            RequestBody data = JsonConvert.DeserializeObject<RequestBody>(requestBody);

            if (account)
            {
                BigQueryTable table = client.GetTable("bigquery-public-data", "google_trends", "top_terms");

                string sql = $"SELECT refresh_date AS Day, term AS Top_Term, rank FROM {table} WHERE rank = 1 AND refresh_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK) GROUP BY Day, Top_Term, rank ORDER BY Day DESC LIMIT 3";
                BigQueryParameter[] parameters = null;
                BigQueryResults results = client.ExecuteQuery(sql, parameters);

                foreach (BigQueryRow row in results)
                {
                    _logger.LogInformation($"{row["Day"]}: {row["Top_Term"]}");
                    data.AdditionalProperties[row["Day"].ToString()] = row["Top_Term"].ToString();
                }
            }

            // Use the deserialized data object as needed
            _logger.LogInformation($"Response data: {JsonConvert.SerializeObject(data)}");

            var jsonResponse = JsonConvert.SerializeObject(data);
            var response = new ContentResult
            {
                Content = jsonResponse,
                ContentType = "application/json",
                StatusCode = (int)System.Net.HttpStatusCode.OK
            };

            return response;
        }
    }

    public class RequestBody
    {
        public int AppId { get; set; }
        public int EventId { get; set; }
        public int SubscriptionId { get; set; }
        public int PortalId { get; set; }
        public long OccurredAt { get; set; }
        public string SubscriptionType { get; set; }
        public int AttemptNumber { get; set; }
        public int ObjectId { get; set; }
        public string ChangeSource { get; set; }
        public string PropertyName { get; set; }
        public string PropertyValue { get; set; }
        public Dictionary<string, string> AdditionalProperties { get; set; } = new Dictionary<string, string>();
    }
}