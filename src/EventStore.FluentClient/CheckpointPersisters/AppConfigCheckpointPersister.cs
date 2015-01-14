using System.Configuration;
using System.Globalization;
using System.Linq;

namespace EventStore.FluentClient.CheckpointPersisters
{
    public class AppConfigCheckpointPersister : IPersistCheckpoint
    {
        private readonly string _checkpointKey;

        public AppConfigCheckpointPersister(string checkpointKey)
        {
            _checkpointKey = checkpointKey;
        }
        
        public void Persist(int? checkpoint)
        {
            var checkpointValue = (checkpoint ?? -1).ToString(CultureInfo.InvariantCulture);

            ConfigurationManager.AppSettings[_checkpointKey] = checkpointValue;

            var config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);

            
            if (!config.AppSettings.Settings.AllKeys.Contains(_checkpointKey))
                config.AppSettings.Settings.Add(_checkpointKey, checkpointValue);
            else
                config.AppSettings.Settings[_checkpointKey].Value = checkpointValue;
            
            config.Save(ConfigurationSaveMode.Modified);
        }

        public int? GetCheckpoint()
        {
            var checkpoint = ConfigurationManager.AppSettings[_checkpointKey];

            if (checkpoint == null)
                return null;

            var checkpointValue = int.Parse(checkpoint);

            return checkpointValue == -1 ? null : (int?) checkpointValue; 
        }
    }
}