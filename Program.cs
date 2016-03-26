namespace ElasticCollectionsDemo
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.ComponentModel;
    using System.Configuration;
    using System.Diagnostics;
    using System.Dynamic;
    using System.Net;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Text.RegularExpressions;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Microsoft.Azure.Documents.Partitioning;
    using Newtonsoft;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using DocumentDB.Samples.Shared.Util;

    /// <summary>
    /// This sample demonstrates how to achieve high performance writes using DocumentDB.
    /// </summary>
    public sealed class Program
    {
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string CollectionName = ConfigurationManager.AppSettings["CollectionName"];
        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp };

        private static readonly int TaskCount = int.Parse(ConfigurationManager.AppSettings["TaskCount"]);
        private static readonly int DefaultConnectionLimit = int.Parse(ConfigurationManager.AppSettings["TaskCount"]);
        private const int MinThreadPoolSize = 20;

        private int count;
        private ConcurrentDictionary<int, double> requestUnitsConsumed = new ConcurrentDictionary<int,double>();

        private DocumentClient client;

        /// <summary>
        /// Initializes a new instance of the <see cref="Program"/> class.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        private Program(DocumentClient client)
        {
            this.client = client;
        }

        /// <summary>
        /// Main method for the sample.
        /// </summary>
        /// <param name="args">command line arguments.</param>
        public static void Main(string[] args)
        {
            ServicePointManager.UseNagleAlgorithm = true;
            ServicePointManager.Expect100Continue = true;
            ServicePointManager.DefaultConnectionLimit = DefaultConnectionLimit;
            ThreadPool.SetMinThreads(MinThreadPoolSize, MinThreadPoolSize);

            PointGenerator.Intialize();

            try
            {
                using (var client = new DocumentClient(new Uri(
                    ConfigurationManager.AppSettings["EndPointUrl"]), 
                    ConfigurationManager.AppSettings["AuthorizationKey"],
                    ConnectionPolicy))
                {
                    var program = new Program(client);
                    program.RunAsync().Wait();
                    Console.WriteLine("Samples completed successfully.");
                }
            }

#if !DEBUG
            catch (Exception e)
            {
                // If the Exception is a DocumentClientException, the "StatusCode" value might help identity 
                // the source of the problem. 
                Console.WriteLine("Samples failed with exception:{0}", e);
            }
#endif

            finally
            {
                Console.WriteLine("End of samples, press any key to exit.");
                Console.ReadKey();
            }
        }

        /// <summary>
        /// Run samples for Order By queries.
        /// </summary>
        /// <returns>a Task object.</returns>
        private async Task RunAsync()
        {
            bool shouldRecreateDatabaseAndCollection = bool.Parse(ConfigurationManager.AppSettings["ShouldDeleteAndRecreateDatabaseAndCollection"]);
            DocumentCollection collection;

            if (shouldRecreateDatabaseAndCollection)
            {
                Console.WriteLine("Creating database " + DatabaseName);
                Database database = await DocumentClientHelper.GetNewDatabaseAsync(this.client, DatabaseName);

                Console.WriteLine("Creating collection " + CollectionName);
                collection = await this.CreatePartitionedCollection(database);
            }
            else
            {
                collection = await this.client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(DatabaseName, CollectionName));
            }

            CreateMetricsCollectionIfNotExists();
            Dictionary<string, object> expando = JsonConvert.DeserializeObject<Dictionary<string, object>>(File.ReadAllText(ConfigurationManager.AppSettings["DocumentTemplateFile"]));

            var tasks = new List<Task>();
            for (var i = 0; i < TaskCount; i++)
            {
                tasks.Add(this.InsertDocument(i, client, collection, expando));
            }

            tasks.Add(this.LogOutputStats());
            await Task.WhenAll(tasks);
        }

        private async Task InsertDocument(int taskId, DocumentClient client, DocumentCollection collection, IDictionary<string, object> sampleDocument)
        {
            requestUnitsConsumed[taskId] = 0;
            string partitionKeyProperty = collection.PartitionKey.Paths[0].Replace("/", "");

            Dictionary<string, object> newDictionary = new Dictionary<string, object>(sampleDocument);

            for (var i = 0; i < 1000000; i++)
            {
                newDictionary["id"] = Guid.NewGuid().ToString();
                newDictionary[partitionKeyProperty] = Guid.NewGuid().ToString();
                newDictionary["location"] = PointGenerator.GetRandomPoint(Guid.NewGuid().GetHashCode());

                try 
                {
                    ResourceResponse<Document> response = await DocumentClientHelper.ExecuteWithRetries<ResourceResponse<Document>>(
                        client,
                        () => client.CreateDocumentAsync(
                            UriFactory.CreateDocumentCollectionUri(DatabaseName, CollectionName),
                            newDictionary,
                            new RequestOptions() { }));

                    requestUnitsConsumed[taskId] += response.RequestCharge;
                    Interlocked.Increment(ref this.count);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Environment.Exit(1);
                }
            }
        }

        private async Task LogOutputStats()
        {
            int lastCount = 0;
            double lastRequestUnits = 0;
            double lastSeconds = 0;

            Stopwatch watch = new Stopwatch();
            watch.Start();

            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                double seconds = watch.Elapsed.TotalSeconds;

                double requestUnits = 0;
                foreach (int taskId in requestUnitsConsumed.Keys)
                {
                    requestUnits += requestUnitsConsumed[taskId];
                }

                int currentCount = this.count;
                double ruPerSecond = (requestUnits / seconds);
                double ruPerMonth = ruPerSecond * 86400 * 30;

                Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                    currentCount, 
                    Math.Round(this.count / seconds),
                    Math.Round(ruPerSecond),
                    Math.Round(ruPerMonth / (1000 * 1000 * 1000)));

                Dictionary<string, object> latestStats = new Dictionary<string, object>();
                latestStats["id"] = "latest";
                latestStats["totalDocumentsCreated"] = currentCount;
                latestStats["documentsCreatedPerSecond"] = Math.Round(this.count / seconds);
                latestStats["requestUnitsPerSecond"] = Math.Round(ruPerSecond);
                latestStats["requestUnitsPerMonth"] = Math.Round(ruPerSecond) * 86400 * 30;
                latestStats["documentsCreatedInLastSecond"] = Math.Round((currentCount - lastCount) / (seconds - lastSeconds));
                latestStats["requestUnitsInLastSecond"] = Math.Round((requestUnits - lastRequestUnits) / (seconds - lastSeconds));
                latestStats["requestUnitsPerMonthBasedOnLastSecond"] =
                    Math.Round(((requestUnits - lastRequestUnits) / (seconds - lastSeconds)) * 86400 * 30);

                try
                {
                    await DocumentClientHelper.ExecuteWithRetries<ResourceResponse<Document>>(
                        client,
                        () => client.UpsertDocumentAsync(
                            UriFactory.CreateDocumentCollectionUri(DatabaseName, ConfigurationManager.AppSettings["MetricsCollectionName"]),
                            latestStats));
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Environment.Exit(1);
                }


                latestStats["id"] = "stats_" + DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss");
                latestStats["ttl"] = 24 * 60 * 60;

                try
                {
                    await DocumentClientHelper.ExecuteWithRetries<ResourceResponse<Document>>(
                        client,
                        () => client.UpsertDocumentAsync(
                            UriFactory.CreateDocumentCollectionUri(DatabaseName, ConfigurationManager.AppSettings["MetricsCollectionName"]),
                            latestStats));
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    Environment.Exit(1);
                }

                lastCount = count;
                lastSeconds = seconds;
                lastRequestUnits = requestUnits;
            }
        }

        /// <summary>
        /// Create a partitioned collection.
        /// </summary>
        /// <returns>The created collection.</returns>
        private async Task<DocumentCollection> CreatePartitionedCollection(Database database)
        {
            DocumentCollection existingCollection = client.CreateDocumentCollectionQuery(database.SelfLink).Where(c => c.Id == CollectionName).AsEnumerable().FirstOrDefault();
            if (existingCollection != null)
            {
                return existingCollection;
            }

            DocumentCollection collection = new DocumentCollection();
            collection.Id = CollectionName;
            collection.PartitionKey.Paths.Add(ConfigurationManager.AppSettings["CollectionPartitionKey"]);
            int collectionThroughput = int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]);

            return await DocumentClientHelper.ExecuteWithRetries<ResourceResponse<DocumentCollection>>(
                this.client,
                () => client.CreateDocumentCollectionAsync(database.SelfLink, collection, new RequestOptions { OfferThroughput = collectionThroughput }));
        }

        private async Task<DocumentCollection> CreateMetricsCollectionIfNotExists()
        {
            string metricsCollectionName = ConfigurationManager.AppSettings["MetricsCollectionName"];

            DocumentCollection existingCollection = client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(DatabaseName))
                .Where(c => c.Id == metricsCollectionName)
                .AsEnumerable().FirstOrDefault();
            
            if (existingCollection != null)
            {
                return existingCollection;
            }

            return await DocumentClientHelper.ExecuteWithRetries<ResourceResponse<DocumentCollection>>(
                this.client,
                () => client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(DatabaseName), 
                    new DocumentCollection { Id = metricsCollectionName }, 
                    new RequestOptions { OfferThroughput = 5000 }));
        }
    }
}