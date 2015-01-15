using System;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.FluentClient.CheckpointPersisters;

namespace EventStore.FluentClient
{
    public class Subscription<T>
    {
        private readonly ConfigurationSettings _settings;
        private string _stream;
        private Action<EventStoreCatchUpSubscription, SubscriptionDropReason, Exception> _onDropped;
        private Action _onConnected;
        private Action<EventStoreCatchUpSubscription> _onLiveProcessingStarted;
        private int _batchSize;
        private int? _checkpoint;
        private bool _durable;
        private ConnectionSettingsBuilder _connectionSettings;
        private IEventStoreConnection _connection;
        private EventStoreStreamCatchUpSubscription _subscription;
        private Action<Event<T>> _onEvent;
        private bool _keepConnection;
        private int _retryCount;
        private TimeSpan _delayBetweenRetry;
        private int _retried;
        private IPersistCheckpoint _persister;
        private bool _connected;

        private Subscription(ConfigurationSettings settings)
        {
            _settings = settings;
            _batchSize = 50;
            _checkpoint = StreamCheckpoint.StreamStart;
        }


        public static Subscription<T> WithSettings(ConfigurationSettings settings)
        {
            return new Subscription<T>(settings);

        }


        public Subscription<T> ToStream(string stream)
        {
            _stream = stream;
            return this;
        }
        public Subscription<T> WithBatchSize(int size)
        {
            if (size < 1)
                throw new ArgumentOutOfRangeException("size", "Batch size must be positive");
            _batchSize = size;
            return this;
        }
        public Subscription<T> FromPosition(int position)
        {
            if (position < 1)
                throw new ArgumentOutOfRangeException("position", "Position must be positive");
            _checkpoint = position;
            return this;
        }
        public Subscription<T> Durable(IPersistCheckpoint persister)
        {
            _durable = true;
            _persister = persister;
            return this;
        }

        public Subscription<T> KeepConnection(RetryCount retryCount, TimeSpan delayBetweenRetry)
        {
            return KeepConnection((int) retryCount, delayBetweenRetry);
        }
        public Subscription<T> KeepConnection(int retryCount, TimeSpan delayBetweenRetry)
        {
            if (retryCount < -1)
                throw new ArgumentOutOfRangeException("retryCount", "Retry count must be greater than -1, where -1 represents NoLimit");
            if (delayBetweenRetry < TimeSpan.FromSeconds(0))
                throw new ArgumentOutOfRangeException("delayBetweenRetry", "Delay between retry must be nonnegative.");
            _retryCount = retryCount;
            _delayBetweenRetry = delayBetweenRetry;
            _keepConnection = true;
            return this;
        }

        public Subscription<T> OnEvent(Action<Event<T>> callback)
        {
            _onEvent = callback;
            return this;
        }
        public Subscription<T> OnDropped(Action<EventStoreCatchUpSubscription, SubscriptionDropReason, Exception> callback)
        {
            _onDropped = callback;
            return this;
        }

        public Subscription<T> OnConnected(Action callback)
        {
            _onConnected = callback;
            return this;
        }

        public Subscription<T> OnLiveProcessingStarted(Action<EventStoreCatchUpSubscription> callback)
        {
            _onLiveProcessingStarted = callback;
            return this;
        }

        private void OnDroppedHandle(EventStoreCatchUpSubscription subscription, SubscriptionDropReason reason, Exception exception)
        {
            _connected = false;
            if (_onDropped != null)
                Task.Run(() => _onDropped(subscription, reason, exception));

            if (!_keepConnection || _retryCount < -1) return;

            if (++_retried > _retryCount && _retryCount != -1) return;

            WaitBeforeNextRetry();
            Restart();
        }
        private void OnLiveProcessingStartedHandle(EventStoreCatchUpSubscription subscription)
        {
            Connected();
            if (_onLiveProcessingStarted != null)
                Task.Run(() => _onLiveProcessingStarted(subscription));
        }


        private void OnEventHandle(EventStoreCatchUpSubscription subscription, ResolvedEvent @event)
        {
            Connected();
            
            _retried = 0;
            _persister.Persist(@event.Event.EventNumber);
                    
            Event<T> deserialized;
            if (Util.TryDeserialize(@event, out deserialized, Defaults.JsonSerializerSettings))
                _onEvent(deserialized);
            
        }
        private void Connected()
        {
            if (_connected) return;

            _connected = true;
            if (_onConnected != null)
                _onConnected();
        }

        public Subscription<T> Start()
        {

            ValidateConfig();

            _connectionSettings = ConnectionSettings.Create()
                .FailOnNoServerResponse()
                .SetDefaultUserCredentials(_settings.Credentials);

            _connection = EventStoreConnection.Create(_connectionSettings, _settings.TcpEndpoint);
            _connection.ConnectAsync().Wait();

            Subscribe();
            
            return this;
        }
        
        private void WaitBeforeNextRetry()
        {
            if (_delayBetweenRetry > TimeSpan.FromTicks(0))
                Task.Delay(_delayBetweenRetry).Wait();
        }

        private void Restart()
        {
            Stop();
            _connection = EventStoreConnection.Create(_connectionSettings, _settings.TcpEndpoint);
            _connection.ConnectAsync().Wait();
            Subscribe();
            
        }

        public Subscription<T> Stop()
        {
            _subscription.Stop();
            _connection.Dispose();

            return this;
        }

        private void ValidateConfig()
        {

            if (_settings == null)
                throw new InvalidOperationException(
                    "ConfigurationSettings are not provided. Use .WithSettings method to provide ConfigurationSettings.");
            if (String.IsNullOrWhiteSpace(_stream))
                throw new InvalidOperationException(
                    "Stream name not provided. Use .ToStream method to provide stream name");
            if (_onEvent == null)
                throw new InvalidOperationException(
                    "Event callback not provided. Use .OnEvent method to provide event callback");
            if (_checkpoint != null && !_durable)
                throw new InvalidOperationException(
                    "Only durable subscription can start form position. Use .Durable method to set durability method");
        }


        private void Subscribe()
        {
            _subscription = _connection.SubscribeToStreamFrom(_stream, _durable ? _persister.GetCheckpoint() : StreamCheckpoint.StreamStart, true,
                OnEventHandle,
                OnLiveProcessingStartedHandle,
                OnDroppedHandle,
                readBatchSize:_batchSize);

        }


    }
}