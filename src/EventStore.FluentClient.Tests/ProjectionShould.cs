using System;
using System.Net.Http;
using System.Threading.Tasks;
using EventStore.FluentClient.Tests.Utils;
using FluentAssertions;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace EventStore.FluentClient.Tests
{
    [TestFixture]
    class ProjectionShould: EsTest
    {

        [SetUp]
        public void SetUp()
        {
            EsProcess.ForceStop();
            EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath").Wait();
        }



        [TearDown]
        public void TearDown()
        {
            EsProcess.RequestStop();
        }



        [Test]
        [Category("Integration")]
        public async Task EnableAllGivenProjections_WhenRequested()
        {

            var settings = ConfigurationSettings.FromConfig("Full");


            await Projection.Enable(settings, SystemProjection.By_Category | SystemProjection.By_Event_Type);


            (await GetProjectionStatus(settings, "$by_category")).Should().Be("Running");
            (await GetProjectionStatus(settings, "$by_event_type")).Should().Be("Running");
            (await GetProjectionStatus(settings, "$stream_by_category")).Should().Be("Stopped");
            (await GetProjectionStatus(settings, "$streams")).Should().Be("Stopped");

        }



        public async Task<string> GetProjectionStatus(ConfigurationSettings settings,string projection)
        {
            var client = new HttpClient();
            var responseForByCategory = await client.GetAsync(String.Format("http://{0}:{1}/projection/{2}", settings.HttpEndpoint.Address, settings.HttpEndpoint.Port, projection));
            var resultForByCategory = await responseForByCategory.Content.ReadAsStringAsync();

            return JObject.Parse(resultForByCategory)["status"].ToString();
        }


        [Test]
        [Category("Integration")]
        public async Task DisableAllGivenProjections_WhenRequested()
        {
            var settings = ConfigurationSettings.FromConfig("Full");

            await Projection.Enable(settings, SystemProjection.By_Category | SystemProjection.By_Event_Type);

            (await GetProjectionStatus(settings, "$by_category")).Should().Be("Running");
            (await GetProjectionStatus(settings, "$by_event_type")).Should().Be("Running");

            await Projection.Disable(settings, SystemProjection.By_Category);

            (await GetProjectionStatus(settings, "$by_category")).Should().Be("Stopped");
            (await GetProjectionStatus(settings, "$by_event_type")).Should().Be("Running");

        }

        [Test]
        [Category("Integration")]
        public async Task CreateProjectionAndRetrieveState_WhenRequested()
        {
            await GenerateSampleStringEvents();

            var settings = ConfigurationSettings.FromConfig("Full");

            await Projection.Enable(settings, SystemProjection.By_Category);

            var projectionName = Guid.NewGuid().ToString();

            await Projection.WithSettings(settings)
                      .WithName(projectionName)
                      .WithFile("SampleProjection.js")
                      .Contnuous()
                      .EmitEnabled()
                      .Enabled()
                      .Create();



            var state = await Projection.GetState(settings, projectionName);

            var count = (int) state["count"];

            count.Should().Be(100);



        }



    }
}