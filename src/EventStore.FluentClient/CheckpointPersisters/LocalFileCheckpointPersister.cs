using System.Globalization;
using System.IO;

namespace EventStore.FluentClient.CheckpointPersisters
{
    public class LocalFileCheckpointPersister : IPersistCheckpoint
    {
        private readonly string _filePath;

        public LocalFileCheckpointPersister(string filePath)
        {
            _filePath = filePath;
        }


        public void Persist(int? checkpoint)
        {
            File.WriteAllText(_filePath, (checkpoint ?? -1).ToString(CultureInfo.InvariantCulture));
        }

        public int? GetCheckpoint()
        {
            if (!File.Exists(_filePath))
                return null;

            var checkpoint = File.ReadAllText(_filePath);

            var checkpointValue = int.Parse(checkpoint);

            return checkpointValue == -1 ? null : (int?)checkpointValue; 
        }
    }
}