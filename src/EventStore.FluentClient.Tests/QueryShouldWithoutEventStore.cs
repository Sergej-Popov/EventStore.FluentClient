using System;
using System.Threading.Tasks;
using NUnit.Framework;

namespace EventStore.FluentClient.Tests
{
    [TestFixture]
    public class QueryShouldWithoutEventStore
    {


        [Test]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task Throw_WhenCreateAttemptedWithoutSettings()
        {
            await Query.WithSettings(null)
                .WithFile("Projections\\SampleProjection.js")
                .Run();

        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task Throw_WhenCreateAttemptedWithoutProjectionFile()
        {
            await Query.WithSettings(ConfigurationSettings.FromConfig("Full"))
                .Run();
        }
    }
}