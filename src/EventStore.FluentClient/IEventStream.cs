using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.ClientAPI;

namespace EventStore.FluentClient
{
    public interface IEventStream
    {
        Task EmitEventAsync<T>(T @event, int expectedVersion = ExpectedVersion.Any, List<KeyValuePair<string, string>> meta = null);
        IEnumerable<Event<T>> ReadForward<T>(int checkpoint = 0, int eventsPerRead = 50);
        IEnumerable<Event<T>> ReadBackward<T>(int eventsPerRead = 50);
    }
}