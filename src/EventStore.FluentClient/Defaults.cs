using Newtonsoft.Json;

namespace EventStore.FluentClient
{
    internal class Defaults
    {
        internal static JsonSerializerSettings JsonSerializerSettings
        {
            get
            {
                return new JsonSerializerSettings
                {
                    DateFormatHandling = DateFormatHandling.IsoDateFormat,
                    DateTimeZoneHandling = DateTimeZoneHandling.Utc
                };
            }
        }
    }
}