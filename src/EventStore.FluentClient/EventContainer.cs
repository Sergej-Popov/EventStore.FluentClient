using System.Collections.Generic;

namespace EventStore.FluentClient
{
    public class EventContainer<T>
    {
        public string ClrType { get; set; }
        public T Data { get; set; }
        public List<KeyValuePair<string, string>> Meta { get; set; }
    }
}