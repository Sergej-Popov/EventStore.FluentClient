using System.Linq;

namespace EventStore.FluentClient.CheckpointPersisters
{
    public class EventStoreCheckpointPersister : IPersistCheckpoint
    {
        private readonly string _streamName;
        private readonly ConfigurationSettings _settings;

        public EventStoreCheckpointPersister(string streamName, ConfigurationSettings settings)
        {
            _streamName = streamName;
            _settings = settings;
        }


        public void Persist(int? checkpoint)
        {
            using (var stream = EventStream.Create(_settings, _streamName).Result)
            {
                stream.EmitEventAsync(checkpoint ?? -1).Wait();
            }
        }

        public int? GetCheckpoint()
        {
            using (var stream = EventStream.Create(_settings, _streamName).Result)
            {
                var checkpoint = stream.ReadBackward<int>(1).FirstOrDefault();

                if (checkpoint == null)
                    return null;

                return checkpoint.Data == -1 ? null : (int?)checkpoint.Data; 
            }
        }        
    }
}