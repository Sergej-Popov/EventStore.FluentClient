using System.Collections.Generic;

namespace EventStore.FluentClient
{
    public class Event<T>
    {
        public T Data { get; set; }
        public List<KeyValuePair<string, string>> Meta { get; set; }
        public int PositionInTargetStream { get; set; }
        public int PositionInSourceStream { get; set; }
        public bool IsResolved { get; set; }

    }
}