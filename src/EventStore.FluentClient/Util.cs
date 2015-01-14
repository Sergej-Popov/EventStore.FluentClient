using System;
using System.Collections.Generic;
using System.Text;
using EventStore.ClientAPI;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace EventStore.FluentClient
{
    internal class Util
    {

        internal static bool TryDeserialize<T>(ResolvedEvent @event, out Event<T> output, JsonSerializerSettings jsonSerializerSettings)
        {

            try
            {

                var containerJson = Encoding.UTF8.GetString(@event.Event.Data);
                var jObject = JObject.Parse(containerJson);
                var typeString = (string)jObject["ClrType"];
                var type = Type.GetType(typeString);
                var dataJson = jObject["Data"].ToString();
                var metaJson = jObject["Meta"].ToString();


                if (typeof (T) == typeof (String))
                    dataJson = String.Format("\"{0}\"", dataJson);
                var data = (T) JsonConvert.DeserializeObject(dataJson, type, jsonSerializerSettings);
                
                
                
                
                var meta = JsonConvert.DeserializeObject<List<KeyValuePair<string, string>>>(metaJson,
                    jsonSerializerSettings);


                output = new Event<T>
                {
                    
                    Data = data,
                    Meta = meta,
                    Position = @event.Event.EventNumber,
                    OriginalPosition = @event.OriginalEventNumber,
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