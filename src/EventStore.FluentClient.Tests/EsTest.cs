using System.Threading.Tasks;

namespace EventStore.FluentClient.Tests
{
    public class EsTest
    {
        protected static async Task GenerateSampleStringEvents()
        {
            using (var stream = await EventStream.Create(ConfigurationSettings.FromConfig("Full"), "QueryTestStream"))
            {
                for (var i = 0; i < 100; i++)
                {
                    await stream.EmitEventAsync("TestEvent");
                }
            }
        }
    }
}