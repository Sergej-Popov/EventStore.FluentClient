using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace EventStore.FluentClient
{
    public class Projection
    {
        private readonly ConfigurationSettings _settings;
        private string _projection;
        private string _name;
        private string _type;
        private bool _enabled;
        private bool _checkpoints;
        private bool _emit;

        public async Task Create()
        {
            if (_settings == null)
                throw new InvalidOperationException(
                    "ConfigurationSettings are not provided. Use .WithSettings method to provide ConfigurationSettings.");

            if (String.IsNullOrWhiteSpace(_name))
                throw new InvalidOperationException(
                    "Projection name not provided. Use .WithName method to provide projection name");

            if (String.IsNullOrWhiteSpace(_type))
                throw new InvalidOperationException(
                    "Projection type not provided. Use .Continuous or .OneTime method to provide projection type");

            if (String.IsNullOrWhiteSpace(_projection))
                throw new InvalidOperationException(
                    "Projection file not provided or empty. Use .WithFile method to provide projection file");



            await PostProjection(_settings, _projection, _type, _enabled, _checkpoints, _emit, _name);
        }


        private Projection(ConfigurationSettings settings)
        {
            _settings = settings;
        }

        public static Projection WithSettings(ConfigurationSettings settings)
        {
            return new Projection(settings);
        }

        public Projection WithName(string name)
        {
            _name = name;
            return this;
        }

        public Projection WithFile(string path)
        {
            var file = new FileInfo(path);
            if (!file.Exists)
                throw new ArgumentException("Path to file is invalid", path);
            _projection = File.ReadAllText(path);
            return this;
        }

        public Projection Contnuous()
        {
            _type = "continuous";
            _checkpoints = true;
            return this;
        }

        public Projection OneTime()
        {
            _type = "onetime";
            return this;
        }

        public Projection Enabled()
        {
            _enabled = true;
            return this;
        }

        public Projection WithCheckpoints()
        {
            _checkpoints = true;
            return this;
        }

        public Projection EmitEnabled()
        {
            _emit = true;
            return this;
        }

        internal static async Task<string> PostProjection(ConfigurationSettings settings, string projection, string type, bool enabled, bool checkpoints, bool emit, string name = null)
        {
            var client = new HttpClient();
            var content = new StringContent(projection);

            
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
                Convert.ToBase64String(Encoding.Default.GetBytes(settings.Credentials.Username + ":" + settings.Credentials.Password)));
            var response =
                await
                    client.PostAsync(
                    String.Format("http://{0}:{1}/projections/{2}?enabled={3}&emit={4}&checkpoints={5}{6}",
                        settings.HttpEndpoint.Address, settings.HttpEndpoint.Port, 
                        type, 
                        enabled ? "yes" : "no", 
                        emit ? "yes" : "no", 
                        checkpoints ? "yes" : "no", 
                        string.IsNullOrEmpty(name) ? "" : "&name=" + name
                    ), content);

            var result = await response.Content.ReadAsStringAsync();

            return JObject.Parse(result)["name"].ToString();
        }



        public static async Task Enable(ConfigurationSettings settings, SystemProjection projections)
        {
            foreach (var projection in GetFlags(projections))
            {
                var projectionName = String.Format("${0}", projection.ToString().ToLower());
                await ChangeProjectionStatus(settings, projectionName, true);
            }
        }

        public static async Task Enable(ConfigurationSettings settings, string projection)
        {
            await ChangeProjectionStatus(settings, projection, true);
        }


        public static async Task Disable(ConfigurationSettings settings, SystemProjection projections)
        {
            foreach (var projection in GetFlags(projections))
            {
                var projectionName = String.Format("${0}", projection.ToString().ToLower());
                await ChangeProjectionStatus(settings, projectionName, false);
            }
        }

        public static async Task Disable(ConfigurationSettings settings, string projection)
        {
            await ChangeProjectionStatus(settings, projection, false);
        }


        private static async Task ChangeProjectionStatus(ConfigurationSettings settings, string projection, bool enable)
        {
            var client = new HttpClient();
            var content = new StringContent(String.Empty);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.Default.GetBytes(settings.Credentials.Username + ":" + settings.Credentials.Password)));

            await client.PostAsync(String.Format("http://{0}:{1}/projection/{2}/command/{3}", settings.HttpEndpoint.Address, settings.HttpEndpoint.Port, projection, enable ? "enable" : "disable"), content);
            
        }



        private static IEnumerable<Enum> GetFlags(Enum input)
        {
            return Enum.GetValues(input.GetType()).Cast<Enum>().Where(input.HasFlag);
        }

        public static async Task<JObject> GetState(ConfigurationSettings settings, string projectionName)
        {
            var result = await GetStateString(settings, projectionName);
            return JObject.Parse(result);
        }
        public static async Task<T> GetState<T>(ConfigurationSettings settings, string projectionName)
        {
            var result = await GetStateString(settings, projectionName);
            return JsonConvert.DeserializeObject<T>(result);
        }
        public static async Task<string> GetStateString(ConfigurationSettings settings, string projectionName)
        {
            var client = new HttpClient();
            var response = await client.GetAsync(String.Format("http://{0}:{1}/projection/{2}/state", settings.HttpEndpoint.Address, settings.HttpEndpoint.Port, projectionName));
            var result = await response.Content.ReadAsStringAsync();
            return result;
        }
        public static async Task<JObject> GetStatus(ConfigurationSettings settings, string name)
        {
            var client = new HttpClient();
            var response = await client.GetAsync(String.Format("http://{0}:{1}/projection/{2}", settings.HttpEndpoint.Address, settings.HttpEndpoint.Port, name));
            var result = await response.Content.ReadAsStringAsync();
            return JObject.Parse(result);
        }

    }

}
