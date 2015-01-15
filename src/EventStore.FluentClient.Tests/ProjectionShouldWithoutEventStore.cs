using System;
using System.Threading.Tasks;
using NUnit.Framework;

namespace EventStore.FluentClient.Tests
{
    [TestFixture]
    public class ProjectionShouldWithoutEventStore
    {


        [Test]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task Throw_WhenCreateAttemptedWithoutSettings()
        {
            await Projection.WithSettings(null)
                .WithFile("Projections\\SampleProjection.js")
                .Contnuous()
                .WithName("SampleProjection")
                .Create();

        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task Throw_WhenCreateAttemptedWithoutName()
        {
            await Projection.WithSettings(ConfigurationSettings.FromConfig("Full"))
                .WithFile("Projections\\SampleProjection.js")
                .Contnuous()
                .Create();
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task Throw_WhenCreateAttemptedWithoutType()
        {
            await Projection.WithSettings(ConfigurationSettings.FromConfig("Full"))
                .WithFile("Projections\\SampleProjection.js")
                .WithName("SampleProjection")
                .Create();
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task Throw_WhenCreateAttemptedWithoutProjectionFile()
        {
            await Projection.WithSettings(ConfigurationSettings.FromConfig("Full"))
                .WithName("SampleProjection")
                .Contnuous()
                .Create();
        }


    }
}