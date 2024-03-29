﻿using System;
using System.Threading.Tasks;
using EventStore.FluentClient.Tests.Utils;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace EventStore.FluentClient.Tests
{
    [TestFixture]
    [Category("Integration")]
    class QueryShould : EsTest
    {

        [TestFixtureSetUp]
        public void SetUp()
        {
            EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath").Wait();

        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            EsProcess.RequestStop();

        }




        [Test]
        [Category("Integration")]
        public async Task RetrieveResulsts_WhenRun()
        {
            await GenerateSampleStringEvents();
            var result = await Query.WithSettings(ConfigurationSettings.FromConfig("Full")).WithFile("Projections\\SampleProjection.js").Run();
            var jResult = JObject.Parse(result);
            Assert.AreEqual(100, (int)jResult["count"]);
        }



        [Test]
        [Category("Integration")]
        public async Task RetrieveResulstsCorrectly_WhenRunningProjectionWithPlaceholders()
        {

            using (var stream = await EventStream.Create(ConfigurationSettings.FromConfig("Full"), "QueryTestStreamForPlaceholderProjection"))
            {
                for (var i = 1; i <= 100; i++)
                {
                    await stream.EmitEventAsync(i > 50 ? DateTime.Now : DateTime.Now.AddDays(-5));
                }
            }
            var result = await Query.WithSettings(ConfigurationSettings.FromConfig("Full")).WithFile("Projections\\SampleProjectionWithDatePlaceHolder.js").Run(DateTime.Now.AddDays(-1).ToString("u"));

            var jResult = JObject.Parse(result);
            Assert.AreEqual(50, (int)jResult["count"]);
        }



        [Test]
        [Category("Integration")]
        [ExpectedException(typeof(ApplicationException))]
        public async Task RetrieveResulsts_WhenRun2()
        {
            await Query.WithSettings(ConfigurationSettings.FromConfig("Full")).WithFile("Projections\\SampleInvalidProjection.js").Run();
        }



    }
}
