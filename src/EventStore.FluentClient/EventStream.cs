using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Newtonsoft.Json;

namespace EventStore.FluentClient
{
    public class EventStream : IDisposable, IEventStream
    {
        private readonly string _streamId;


        public JsonSerializerSettings JsonSerializerSettings;

        private readonly IEventStoreConnection _connection;

        private EventStream(IEventStoreConnection connection, string streamId)
        {
            _connection = connection;
            _streamId = streamId;

            JsonSerializerSettings = Defaults.JsonSerializerSettings;

        }



        public static async Task<EventStream> Create(ConfigurationSettings settings, string streamId)
        {

            var connectionSettings = ConnectionSettings.Create()
                .FailOnNoServerResponse()
                .SetDefaultUserCredentials(settings.Credentials);


            var stream = new EventStream(EventStoreConnection.Create(connectionSettings, settings.TcpEndpoint), streamId);


            await stream._connection.ConnectAsync();


            return stream;
        }


        public void Dispose()
        {
            _connection.Close();
        }




        public async Task EmitEventAsync<T>(T @event, int expectedVersion = ExpectedVersion.Any, object meta = null)
        {

            var dataJson = JsonConvert.SerializeObject(@event, JsonSerializerSettings);
            var dataBytes = Encoding.UTF8.GetBytes(dataJson);
            byte[] metaBytes = null;
            if (meta != null)
            {
                var metaJson = JsonConvert.SerializeObject(meta, JsonSerializerSettings);
                metaBytes = Encoding.UTF8.GetBytes(metaJson);
            }

            await _connection.AppendToStreamAsync(_streamId, expectedVersion, new List<EventData>
            {
                new EventData(Guid.NewGuid(),  @event.GetType().Name, true, dataBytes, metaBytes)
            });


        }


        public IEnumerable<Event<T>> ReadForward<T>(int checkpoint = 0, int eventsPerRead = 50)
        {

            if (checkpoint < 0)
                throw new InvalidOperationException("Cannot get checkpoint < 0");


            var nextEventNumber = checkpoint;

            StreamEventsSlice currentSlice;
            do
            {

                try
                {
                    currentSlice = _connection.ReadStreamEventsForwardAsync(_streamId, nextEventNumber, eventsPerRead, true).Result;
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception.Message);
                    throw;
                }
                if (currentSlice.Status == SliceReadStatus.StreamNotFound)
                    throw new StreamNotFoundException(_streamId);

                if (currentSlice.Status == SliceReadStatus.StreamDeleted)
                    throw new StreamDeletedException(_streamId);

                nextEventNumber = currentSlice.NextEventNumber;

                foreach (var evnt in currentSlice.Events)
                {
                    Event<T> deserialized;
                    if (Util.TryDeserialize(evnt, out deserialized, JsonSerializerSettings))
                        yield return deserialized;
                }
            } while (!currentSlice.IsEndOfStream);
        }

        public IEnumerable<Event<T>> ReadBackward<T>(int eventsPerRead = 50)
        {
            var nextEventNumber = StreamPosition.End;

            StreamEventsSlice currentSlice;
            do
            {

                try
                {
                    currentSlice = _connection.ReadStreamEventsBackwardAsync(_streamId, nextEventNumber, eventsPerRead, true).Result;
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception.Message);
                    throw;
                }
                if (currentSlice.Status == SliceReadStatus.StreamNotFound)
                    throw new StreamNotFoundException(_streamId);

                if (currentSlice.Status == SliceReadStatus.StreamDeleted)
                    throw new StreamDeletedException(_streamId);

                nextEventNumber = currentSlice.NextEventNumber;


                foreach (var evnt in currentSlice.Events)
                {
                    Event<T> deserialized;
                    if (Util.TryDeserialize(evnt, out deserialized, JsonSerializerSettings))
                        yield return deserialized;
                }

            } while (!currentSlice.IsEndOfStream);


        }







        public class StreamDeletedException : Exception
        {
            public StreamDeletedException(string streamId)
                : base(string.Format("Stream requested has been deleted {0}", streamId))
            {

            }
        }
        public class StreamNotFoundException : Exception
        {
            public StreamNotFoundException(string streamId)
                : base(string.Format("Could not find stream for id {0}", streamId))
            {

            }
        }


    }
}
