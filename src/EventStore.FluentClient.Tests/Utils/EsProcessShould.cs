using System;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using NUnit.Framework;

namespace EventStore.FluentClient.Tests.Utils
{
    [TestFixture]
    public class EsProcessShould
    {

        [TearDown]
        public void TestTearDown()
        {
            EsProcess.ForceStop();
        }


        [Test]
        [Category("Integration")]
        public async Task StartES_WhenRequested()
        {

            await EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath");

            await TestConnection();
        }

        [Test]
        [Category("Integration")]
        [ExpectedException(typeof(ConnectionClosedException))]
        public async Task StopES_WhenRequested()
        {
            await EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath");
            EsProcess.RequestStop();
            await TestConnection();
        }

        [Test]
        [Category("Integration")]
        [ExpectedException(typeof(ConnectionClosedException))]
        public async Task ForceStopES_WhenMultipleStartsRequested()
        {
            await EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath");
            await EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath");
            await EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath");
            EsProcess.ForceStop();
            await TestConnection();
        }



        private async Task<bool> TestConnection()
        {
            var connectionSettings = ConnectionSettings.Create()
                .FailOnNoServerResponse()
                .SetOperationTimeoutTo(TimeSpan.FromSeconds(1))
                .LimitRetriesForOperationTo(1)
                .SetDefaultUserCredentials(new UserCredentials("admin", "changeit"));

            var connection = EventStoreConnection.Create(connectionSettings, new IPEndPoint(IPAddress.Parse("127.0.0.1"), 2118 ));

            await connection.ConnectAsync();

            await connection.AppendToStreamAsync("TestStream", ExpectedVersion.Any, new EventData(Guid.NewGuid(), "String", true, Encoding.UTF8.GetBytes(@"{""Message"":""Hello ES""}"),null));

            await connection.ReadStreamEventsForwardAsync("TestStream", 0, 1, false);

            return true;

        }
    }
}
