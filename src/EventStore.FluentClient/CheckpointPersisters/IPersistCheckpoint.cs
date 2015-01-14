namespace EventStore.FluentClient.CheckpointPersisters
{
    public interface IPersistCheckpoint
    {
        void Persist(int? checkpoint);

        int? GetCheckpoint();
    }
}