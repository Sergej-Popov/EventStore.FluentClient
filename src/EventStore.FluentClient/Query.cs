using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace EventStore.FluentClient
{
    public class Query
    {
        private readonly ConfigurationSettings _settings;
        private string _projection;

        private Query(ConfigurationSettings settings)
        {
            _settings = settings;
        }



        public static Query WithSettings(ConfigurationSettings settings)
        {
            return new Query(settings);

        }

        public Query WithFile(string path)
        {
            var file = new FileInfo(path);
            if (!file.Exists)
                throw new ArgumentException("Path to file is invalid", path);
            _projection = File.ReadAllText(path);
            return this;
        }
        
        
        public async Task<T> Run<T>(TimeSpan timeout, params string[] args)
        {
            var result = await Run(timeout, args);
            return JsonConvert.DeserializeObject<T>(result);
        }

        public async Task<T> Run<T>(params string[] args)
        {
            var result = await Run(args);
            return JsonConvert.DeserializeObject<T>(result);
        }

        public async Task<string> Run(TimeSpan timeout, params string[] args)
        {
            Verify();

            if (args != null && args.Length > 0)
            {
                Enumerable.Range(0, args.Length).ToList().ForEach(i =>
                {
                    _projection = _projection.Replace("$" + i + "$", args[i]);
                });
            }

            var guid = await Projection.PostProjection(_settings, _projection, "transient", true, false, false);

            await AwaitQueryCompletion(guid, timeout);

            return await Projection.GetStateString(_settings, guid);
        }

        private void Verify()
        {
            if (_settings == null)
                throw new InvalidOperationException(
                    "ConfigurationSettings are not provided. Use .WithSettings method to provide ConfigurationSettings.");
            if (String.IsNullOrWhiteSpace(_projection))
                throw new InvalidOperationException(
                    "Query file not provided or empty. Use .WithFile method to provide query file");
        }

        public async Task<string> Run(params string[] args)
        {
            return await Run(TimeSpan.FromSeconds(3), args);
        }




        private async Task AwaitQueryCompletion(string guid, TimeSpan timeout)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            var status = await Projection.GetStatus(_settings, guid);

            while (status["status"].ToString() != "Completed/Stopped/Writing results" || status["progress"].ToString() != "100")
            {

                if (status["status"].ToString().Contains("Faulted"))
                    throw new ApplicationException(status["stateReason"].ToString());

                if (sw.Elapsed > timeout)
                    throw new TimeoutException("Query operation timed out");
                status = await Projection.GetStatus(_settings, guid);
                await Task.Delay(100);
            }
        }


    }
}