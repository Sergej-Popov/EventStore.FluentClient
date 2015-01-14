using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using EventStore.FluentClient.Tests.Utils;
using FluentAssertions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace EventStore.FluentClient.Tests
{



    [TestFixture]
    [Category("Integration")]
    public class ClientShould
    {

        string _stream;
        private IEventStoreConnection _eventStoreConnection;

        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath").Wait();
        }


        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            EsProcess.RequestStop();
        }




        [SetUp]
        public void TestSetUp()
        {
            _stream = String.Format("TestStream-{0}", Guid.NewGuid());
            

            var settings = ConfigurationSettings.FromConfig("Full");
            var connectionSettings = ConnectionSettings.Create()
                .FailOnNoServerResponse()
                .SetDefaultUserCredentials(settings.Credentials);
            _eventStoreConnection = EventStoreConnection.Create(connectionSettings, settings.TcpEndpoint);
        }



        [Test]
        [Category("Integration")]
        public async Task WriteEventToEventStore_WhenEmitCalled()
        {

            var originalEvent = new SampleEvent
            {
                Id = "123",
                CreatedOn = DateTime.UtcNow
            };
            using (var eventStream = await EventStream.Create(ConfigurationSettings.FromConfig("Full"), _stream))
            {
                await eventStream.EmitEventAsync(originalEvent);
            }
            

            await _eventStoreConnection.ConnectAsync();
            var slice = await _eventStoreConnection.ReadStreamEventsForwardAsync(_stream, 0, 100, false);
            _eventStoreConnection.Close();

            slice.Events.Length.Should().Be(1);

            var dataJson = JObject.Parse(Encoding.UTF8.GetString(slice.Events[0].Event.Data))["Data"].ToString();
            var retrievedEvent = JsonConvert.DeserializeObject<SampleEvent>(dataJson);

            retrievedEvent.CreatedOn.Should().Be(originalEvent.CreatedOn);
            retrievedEvent.Id.Should().Be(originalEvent.Id);

        }

        [Test]
        [Category("Integration")]
        public async Task WriteMetaDataCorrectly_WhenEmitCalled()
        {

            var originalEvent = new SampleEvent();
            using (var eventStream = await EventStream.Create(ConfigurationSettings.FromConfig("Full"), _stream))
            {
                await eventStream.EmitEventAsync(originalEvent);
            }


            await _eventStoreConnection.ConnectAsync();
            var slice = await _eventStoreConnection.ReadStreamEventsForwardAsync(_stream, 0, 100, false);
            _eventStoreConnection.Close();

            slice.Events.Length.Should().Be(1);


            ((string)JObject.Parse(Encoding.UTF8.GetString(slice.Events[0].Event.Data))["ClrType"]).Should().Be("EventStore.FluentClient.Tests.SampleEvent, EventStore.FluentClient.Tests");


        }


        [Test]
        [Category("Integration")]
        [ExpectedException(typeof(AggregateException))]
        public void ShouldThrowAggregateExceptionWhenCredentialsAreWrong()
        {
            var originalEvent = new SampleEvent();

            var goodConfig = ConfigurationSettings.FromConfig("Full");

            var badConfig = new ConfigurationSettings
            {
                Credentials = new UserCredentials("wrong", "wrong"),
                HttpEndpoint = goodConfig.HttpEndpoint,
                TcpEndpoint = goodConfig.TcpEndpoint
            };
               

            using (var eventStream =  EventStream.Create(badConfig, _stream).Result)
            {
                eventStream.EmitEventAsync(originalEvent).Wait();
            }
        }

        [Test]
        [Category("Integration")]
        [ExpectedException(typeof(NotAuthenticatedException))]
        public async Task ShouldThrowAggregateExceptionWhenCredentialsAreWrong_ASync()
        {
            var originalEvent = new SampleEvent();

            var goodConfig = ConfigurationSettings.FromConfig("Full");

            var badConfig = new ConfigurationSettings
            {
                Credentials = new UserCredentials("wrong", "wrong"),
                HttpEndpoint = goodConfig.HttpEndpoint,
                TcpEndpoint = goodConfig.TcpEndpoint
            };


            using (var eventStream = await EventStream.Create(badConfig, _stream))
            {
                await eventStream.EmitEventAsync(originalEvent);
            }
        }

        [Test]
        [Category("Integration")]
        [ExpectedException(typeof(ConnectionClosedException))]
        public async Task ShouldThrowAggregateExceptionWhenIpWrong_ASync()
        {
            var originalEvent = new SampleEvent();

            var goodConfig = ConfigurationSettings.FromConfig("Full");

            var badConfig = new ConfigurationSettings
            {
                Credentials = goodConfig.Credentials,
                HttpEndpoint = new IPEndPoint(IPAddress.Parse("10.0.0.1"), 8080),
                TcpEndpoint = new IPEndPoint(IPAddress.Parse("10.0.0.1"), 8080)
            };


            using (var eventStream = await EventStream.Create(badConfig, _stream))
            {
                await eventStream.EmitEventAsync(originalEvent);
            }
        }


        [Test]
        [Category("Integration")]
        [ExpectedException(typeof(AggregateException))]
        public void ShouldThrowAggregateExceptionWhenIpWrong()
        {
            var originalEvent = new SampleEvent();

            var goodConfig = ConfigurationSettings.FromConfig("Full");

            var badConfig = new ConfigurationSettings
            {
                Credentials = goodConfig.Credentials,
                HttpEndpoint = new IPEndPoint(IPAddress.Parse("10.0.0.1"), 8080),
                TcpEndpoint = new IPEndPoint(IPAddress.Parse("10.0.0.1"), 8080)
            };


            using (var eventStream = EventStream.Create(badConfig, _stream).Result)
            {
                 eventStream.EmitEventAsync(originalEvent).Wait();
            }
        }


        [Test]
        [Category("Integration")]
        public async Task GetEventFromEventStore_WhenRetrieveCalled()
        {

            var originalEvent = new EventContainer<SampleEvent>
            {
                ClrType = "EventStore.FluentClient.Tests.SampleEvent, EventStore.FluentClient.Tests",
                Data = new SampleEvent
                {
                    Id = "123",
                    CreatedOn = DateTime.UtcNow
                },
                Meta = null
            };

            
            var json = JsonConvert.SerializeObject(originalEvent);
            var bytes = Encoding.UTF8.GetBytes(json);
            await _eventStoreConnection.ConnectAsync();
            await _eventStoreConnection.AppendToStreamAsync(_stream, ExpectedVersion.EmptyStream,
                                    new EventData(Guid.NewGuid(), originalEvent.GetType().FullName, true, bytes, null));
            _eventStoreConnection.Close();


            List<SampleEvent> retrievedEvents;
            using (EventStream eventStream = await EventStream.Create(ConfigurationSettings.FromConfig("Full"), _stream))
            {
                retrievedEvents = eventStream.ReadForward<SampleEvent>().Select(e => e.Data).ToList();
            }

            retrievedEvents.Should().HaveCount(1);
            retrievedEvents.First().CreatedOn.Should().Be(originalEvent.Data.CreatedOn);
            retrievedEvents.First().Id.Should().Be(originalEvent.Data.Id);

        }

        [TestCase(2, 4)]
        [TestCase(5, 10)]
        [TestCase(1, 1)]
        [Category("Integration")]
        public async Task GetLastRetrievedItem_WhenReadingBackFowardAndTakingLast(int eventsPerRead, int skip)
        {

            var expectedEvents = GetListOfSampleEvents().ToList();
            await PopulateStream(expectedEvents);

            using (var eventStream = await EventStream.Create(ConfigurationSettings.FromConfig("Full"), _stream))
            {

                var result = eventStream.ReadBackward<SampleEvent>(eventsPerRead).Skip(skip).First();

                result.Data.ShouldBeEquivalentTo(expectedEvents.ElementAt(expectedEvents.Count - skip - 1));

            }
        }

        [Test]
        [Category("Integration")]
        public async Task EvaluatesSkipAndFirstWithReadingForward()
        {

            var expectedEvents = GetListOfSampleEvents().ToList();
            await PopulateStream(expectedEvents);

            using (var eventStream = await EventStream.Create(ConfigurationSettings.FromConfig("Full"), _stream))
            {

                var result = eventStream.ReadForward<SampleEvent>(eventsPerRead:2).Skip(10).First();

                result.Data.ShouldBeEquivalentTo(expectedEvents.ElementAt(10));

            }


        }

        [Test]
        [Category("Integration")]
        public async Task EvaluateSingleConditionCorrectlyWithForwardRead()
        {
            var expectedEvents = GetListOfSampleEvents().ToList();
            await PopulateStream(expectedEvents);

            using (var eventStream = await EventStream.Create(ConfigurationSettings.FromConfig("Full"), _stream))
            {
                var result = eventStream.ReadForward<SampleEvent>(eventsPerRead: 2).Single(se => se.Data.CreatedOn.Day == DateTime.UtcNow.AddDays(-5).Day);
                result.Data.ShouldBeEquivalentTo(expectedEvents.ElementAt(4));
            }

        }


        [Test]
        [Category("Integration")]
        public async Task EvaluateSingleConditionCorrectlyWithBackwardsRead()
        {
            var expectedEvents = GetListOfSampleEvents().ToList();
            await PopulateStream(expectedEvents);

            using (var eventStream = await EventStream.Create(ConfigurationSettings.FromConfig("Full"), _stream))
            {
                var result = eventStream.ReadBackward<SampleEvent>(eventsPerRead: 2).Single(se => se.Data.CreatedOn.Day == DateTime.UtcNow.AddDays(-5).Day);
                result.Data.ShouldBeEquivalentTo(expectedEvents.ElementAt(4));
            }

        }




        private IEnumerable<SampleEvent> GetListOfSampleEvents()
        {

            var list = new List<SampleEvent>();

            Enumerable.Range(1,30).ToList().ForEach(i => list.Add(new SampleEvent
            {
                Id = Guid.NewGuid().ToString(),
                CreatedOn = DateTime.UtcNow.AddDays(-i)
            }));

            return list;
        }
        
        private async Task PopulateStream(IEnumerable<SampleEvent> events, string stream = null)
        {

            var eventData = events.Select(ev => new EventData(Guid.NewGuid(), ev.GetType().Name, true, Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new EventContainer<SampleEvent>
            {
                ClrType = "EventStore.FluentClient.Tests.SampleEvent, EventStore.FluentClient.Tests",
                Data = ev,
                Meta = null
            })), null));
            await _eventStoreConnection.ConnectAsync();
            await _eventStoreConnection.AppendToStreamAsync(stream ?? _stream, ExpectedVersion.Any, eventData);
            _eventStoreConnection.Close();



        }


        public static void VerifyWait(Action action, TimeSpan timeout)
        {
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();


            while (sw.Elapsed < timeout)
            {
                try
                {

                    action();
                    return;
                }
                catch
                {
                    Task.Delay(timeout.Milliseconds / 5).Wait();
                }
            }
            sw.Stop();
            action();
        }
      

     

     
    

    }
}
