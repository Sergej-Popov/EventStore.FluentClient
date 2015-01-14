using System.Collections.Generic;

namespace EventStore.FluentClient
{
    public class Event<T>
    {
        public T Data { get; set; }
        public List<KeyValuePair<string, string>> Meta { get; set; }
        public int Position { get; set; }
        public int OriginalPosition { get; set; }
        public bool IsResolved { get; set; }

    }
}