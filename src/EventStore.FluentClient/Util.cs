using System.Collections.Generic;
using System.Text;
using EventStore.ClientAPI;
using Newtonsoft.Json;

namespace EventStore.FluentClient
{
    internal class Util
    {

        internal static bool TryDeserialize<T>(ResolvedEvent @event, out Event<T> output, JsonSerializerSettings jsonSerializerSettings)
        {

            try
            {

                var dataJson = Encoding.UTF8.GetString(@event.Event.Data);
                var metaJson = Encoding.UTF8.GetString(@event.Event.Metadata);
                List<KeyValuePair<string, string>> meta;


                T data;
                try
                {
                    data = JsonConvert.DeserializeObject<T>(dataJson, jsonSerializerSettings);
                }
                catch
                {
                    output = null;
                    return false;
                }

                try
                {
                    meta = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(metaJson, jsonSerializerSettings);
                }
                catch
                {
                    meta = new List<KeyValuePair<string, string>>();
                }
                
                
                


                output = new Event<T>
                {
                    
                    Data = data,
                    Meta = meta,
                    PositionInSourceStream = @event.Event.EventNumber,
                    PositionInTargetStream = @event.OriginalEventNumber,
                    IsResolved = @event.IsResolved
                };
                return true;
            }
            catch
            {
                output = null;
                return false;
            }
        }

    }
}