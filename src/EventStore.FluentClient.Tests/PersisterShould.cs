using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using EventStore.FluentClient.CheckpointPersisters;
using EventStore.FluentClient.Tests.Utils;
using FluentAssertions;
using NUnit.Framework;

namespace EventStore.FluentClient.Tests
{
    [TestFixture]
    class PersisterShould
    {
        private const string CheckpointKey = "ES:SubscriptionCheckpoint";
        private const string CheckpointFile = "Checkpoint.txt";
        private const string CheckpointStream = "CheckpointTestStream";

        [SetUp]
        public void SetUp()
        {

        }

        [TearDown]
        public void TearDown()
        {
            EsProcess.ForceStop();
        }

        [Test]
        public void PersistCheckpointCorrectly_ToAppConfig()
        {
            var persister = new AppConfigCheckpointPersister(CheckpointKey);

            persister.Persist(7);

            var checkpointValue = ConfigurationManager.AppSettings[CheckpointKey];

            checkpointValue.Should().Be("7");
        }

        [Test]
        public void PersistNullCheckpointCorrectly_ToAppConfig()
        {
            var persister = new AppConfigCheckpointPersister(CheckpointKey);

            persister.Persist(null);

            var checkpointValue = ConfigurationManager.AppSettings[CheckpointKey];

            checkpointValue.Should().Be("-1");
        }

        [Test]
        public void PersistCheckpointCorrectly_ToLocalFile()
        {
            var persister = new LocalFileCheckpointPersister(CheckpointFile);

            persister.Persist(8);

            File.Exists(CheckpointFile).Should().BeTrue();

            File.ReadAllText(CheckpointFile).Should().Be("8");

        }

        [Test]
        public void PersistNullCheckpointCorrectly_ToLocalFile()
        {
            var persister = new LocalFileCheckpointPersister(CheckpointFile);

            persister.Persist(null);

            File.Exists(CheckpointFile).Should().BeTrue();

            File.ReadAllText(CheckpointFile).Should().Be("-1");

        }

        [Test]
        [Category("Integration")]
        public async Task PersistCheckpointCorrectly_ToEventStore()
        {
            EsProcess.ForceStop();
            EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath").Wait();
            ResetSubscriptionCheckpoint();

            var settings = ConfigurationSettings.FromConfig("Full");


            var persister = new EventStoreCheckpointPersister(CheckpointStream, settings);

            persister.Persist(9);

            using (var stream = await EventStream.Create(settings, CheckpointStream))
            {
                stream.ReadBackward<int>(1).Single().Data.Should().Be(9);

            }
        }

        [Test]
        [Category("Integration")]
        public async Task PersistNullCheckpointCorrectly_ToEventStore()
        {
            EsProcess.ForceStop();
            EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath").Wait();
            ResetSubscriptionCheckpoint();

            var settings = ConfigurationSettings.FromConfig("Full");


            var persister = new EventStoreCheckpointPersister(CheckpointStream, settings);

            persister.Persist(null);

            using (var stream = await EventStream.Create(settings, CheckpointStream))
            {
                stream.ReadBackward<int>(1).Single().Data.Should().Be(-1);

            }
        }

        [Test]
        public void RetrieveLatestCheckpoint_FromAppConfig()
        {
            var persister = new AppConfigCheckpointPersister(CheckpointKey);

            persister.Persist(7);
            persister.Persist(8);
            persister.Persist(9);

            persister.GetCheckpoint().Should().Be(9);
        }

        [Test]
        public void RetrieveLatestCheckpoint_FromLocalFile()
        {
            var persister = new LocalFileCheckpointPersister(CheckpointFile);

            persister.Persist(7);
            persister.Persist(8);
            persister.Persist(9);

            persister.GetCheckpoint().Should().Be(9);
        }

        [Test]
        [Category("Integration")]
        public void RetrieveLatestCheckpoint_FromEventStore()
        {
            EsProcess.ForceStop();
            EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath").Wait();
            ResetSubscriptionCheckpoint();

            var settings = ConfigurationSettings.FromConfig("Full");


            var persister = new EventStoreCheckpointPersister(CheckpointStream, settings);

            persister.Persist(7);
            persister.Persist(8);
            persister.Persist(9);

            persister.GetCheckpoint().Should().Be(9);
        }
        
        private void ResetSubscriptionCheckpoint()
        {
            ConfigurationManager.AppSettings[CheckpointKey] = "-1";
            var config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            config.AppSettings.Settings["ES:SubscriptionCheckpoint"].Value = "-1";
            config.Save(ConfigurationSaveMode.Modified);
        }
    }
}
