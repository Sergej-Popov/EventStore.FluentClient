using System;

namespace EventStore.FluentClient.Tests
{
    public class SampleEvent
    {
        public SampleEvent()
        {
            Id = Guid.NewGuid().ToString();
            CreatedOn = DateTime.UtcNow;
        }

        public string Id { get; set; }
        public DateTime CreatedOn { get; set; }
    }
}