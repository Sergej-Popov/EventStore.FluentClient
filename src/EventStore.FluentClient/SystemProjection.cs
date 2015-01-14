using System;

namespace EventStore.FluentClient
{
    [Flags]
    public enum SystemProjection
    {
        // ReSharper disable InconsistentNaming
        By_Category = 1,
        By_Event_Type = 2,
        Stream_By_Category = 4,
        Streams = 8,
        Users = 16
        // ReSharper restore InconsistentNaming
    }
}