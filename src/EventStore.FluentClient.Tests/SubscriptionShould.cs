using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using EventStore.FluentClient.CheckpointPersisters;
using EventStore.FluentClient.Tests.Utils;
using FluentAssertions;
using NUnit.Framework;

namespace EventStore.FluentClient.Tests
{
    [TestFixture]
    internal class SubscriptionShould : EsTest
    {

        [SetUp]
        public void SetUp()
        {
            EsProcess.ForceStop();
            EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath").Wait();
            ResetSubscriptionCheckpoint();
        }



        [TearDown]
        public void TearDown()
        {
            EsProcess.RequestStop();
        }


        [Test]
        [Category("Integration")]
        public async Task Resubscribe_WhenConnectionDropped()
        {
            
            Trace.WriteLine(DateTime.Now.ToString("T") + " Test");


            var settings = ConfigurationSettings.FromConfig("Full");
            Trace.WriteLine(DateTime.Now.ToString("T") + " Sending Event1.");
            using (var stream = await EventStream.Create(settings, "SubscriptionTestStream"))
            {
                await stream.EmitEventAsync("Event1");
            }

            var notifications = new List<Event<String>>();


            // ReSharper disable NotAccessedVariable
            int droppedCount = 0;
            int connecedCount = 0;
            // ReSharper restore NotAccessedVariable

            Trace.WriteLine(DateTime.Now.ToString("T") + " Starting Subscription");
            var subscription = Subscription<String>.WithSettings(settings)
                                                   .ToStream("SubscriptionTestStream")
                                                   .Durable(new AppConfigCheckpointPersister("ES:SubscriptionCheckpoint"))
                                                   .KeepConnection(RetryCount.NoLimit, TimeSpan.FromSeconds(1))
                                                   .OnConnected(() =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " Connected.");
                                                       connecedCount++;
                                                   })
                                                   .OnDropped((upSubscription, reason, arg3) =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " Dropped. Reason: " + reason);
                                                       droppedCount++;
                                                   })
                                                   .OnEvent(@event =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " OnEvent. Num: " + @event.PositionInTargetStream);
                                                       notifications.Add(@event);
                                                   })
                                                   .OnLiveProcessingStarted(upSubscription => Trace.WriteLine(DateTime.Now.ToString("T") + " Live processing started"))
                                                   .Start();





            await Task.Delay(5000);


            Trace.WriteLine(DateTime.Now.ToString("T") + " Asserting for 1 event");
            notifications.Single().Should().Match<Event<String>>(note => note.Data == "Event1");
            Trace.WriteLine(DateTime.Now.ToString("T") + " Assert OK.");



            Trace.WriteLine(DateTime.Now.ToString("T") + " Stopping ES.");
            EsProcess.ForceStop();
            await Task.Delay(5000);
            Trace.WriteLine(DateTime.Now.ToString("T") + " Restarting ES.");
            await EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath");


            Trace.WriteLine(DateTime.Now.ToString("T") + " Sending Event2.");
            using (var stream = await EventStream.Create(settings, "SubscriptionTestStream"))
            {
                await stream.EmitEventAsync("Event1");
                await stream.EmitEventAsync("Event2");
            }

            await Task.Delay(5000);

            Trace.WriteLine(DateTime.Now.ToString("T") + " Asserting for 2 events");
            notifications.First().Should().Match<Event<String>>(note => note.Data == "Event1");
            notifications.Last().Should().Match<Event<String>>(note => note.Data == "Event2");
            notifications.Should().HaveCount(2);
            Trace.WriteLine(DateTime.Now.ToString("T") + " Assert OK.");




            Trace.WriteLine(DateTime.Now.ToString("T") + " Happy End.");


            connecedCount.Should().BeGreaterOrEqualTo(2);
            droppedCount.Should().BeGreaterOrEqualTo(1);

            subscription.Stop();


        }


        [Test]
        [Category("Integration")]
        public async Task ResubscribeToProjectedStream_WhenConnectionDropped()
        {

            Trace.WriteLine(DateTime.Now.ToString("T") + " Test");


            var settings = ConfigurationSettings.FromConfig("Full");
            Trace.WriteLine(DateTime.Now.ToString("T") + " Sending Event1.");
            using (var stream = await EventStream.Create(settings, "LinkToProjectionOriginalStream-1"))
            {
                await stream.EmitEventAsync(new SampleEvent{CreatedOn = DateTime.Now, Id = "1"});
            }

            var notifications = new List<Event<SampleEvent>>();


            // ReSharper disable NotAccessedVariable
            int droppedCount = 0;
            int connecedCount = 0;
            // ReSharper restore NotAccessedVariable


            await Projection.Enable(settings, SystemProjection.By_Category);
            await Projection.WithSettings(settings)
                      .Contnuous()
                      .Enabled()
                      .WithName("LinkToProjection")
                      .WithFile("Projections\\LinkToSampleProjection.js")
                      .EmitEnabled()
                      .Create();





            Trace.WriteLine(DateTime.Now.ToString("T") + " Starting Subscription");
            var subscription = Subscription<SampleEvent>.WithSettings(settings)
                                                   .ToStream("ProjectedStream")
                                                   .Durable(new AppConfigCheckpointPersister("ES:SubscriptionCheckpoint"))
                                                   .KeepConnection(RetryCount.NoLimit, TimeSpan.FromSeconds(1))
                                                   .OnConnected(() =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " Connected.");
                                                       connecedCount++;
                                                   })
                                                   .OnDropped((upSubscription, reason, arg3) =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " Dropped. Reason: " + reason);
                                                       droppedCount++;
                                                   })
                                                   .OnEvent(@event =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " OnEvent. Num: " + @event.PositionInTargetStream);
                                                       notifications.Add(@event);
                                                   })
                                                   .OnLiveProcessingStarted(upSubscription => Trace.WriteLine(DateTime.Now.ToString("T") + " Live processing started"))
                                                   .Start();





            await Task.Delay(5000);


            Trace.WriteLine(DateTime.Now.ToString("T") + " Asserting for 1 event");
            notifications.Single().Should().Match<Event<SampleEvent>>(note => note.Data.Id == "1");
            Trace.WriteLine(DateTime.Now.ToString("T") + " Assert OK.");



            Trace.WriteLine(DateTime.Now.ToString("T") + " Stopping ES.");
            EsProcess.ForceStop();
            await Task.Delay(5000);
            Trace.WriteLine(DateTime.Now.ToString("T") + " Restarting ES.");
            await EsProcess.RequestStart("ES:InMemoryStoreSettings", "ES:ExePath");

            await Projection.Enable(settings, SystemProjection.By_Category);
            await Projection.WithSettings(settings)
                      .Contnuous()
                      .Enabled()
                      .WithName("LinkToProjection")
                      .WithFile("Projections\\LinkToSampleProjection.js")
                      .EmitEnabled()
                      .Create();

            Trace.WriteLine(DateTime.Now.ToString("T") + " Sending Event2.");
            using (var stream = await EventStream.Create(settings, "LinkToProjectionOriginalStream-2"))
            {
                await stream.EmitEventAsync(new SampleEvent { CreatedOn = DateTime.Now, Id = "1" });
            }
            using (var stream = await EventStream.Create(settings, "LinkToProjectionOriginalStream-2"))
            {
                await stream.EmitEventAsync(new SampleEvent { CreatedOn = DateTime.Now, Id = "2" });
            }

            await Task.Delay(5000);

            Trace.WriteLine(DateTime.Now.ToString("T") + " Asserting for 2 events");
            notifications.First().Should().Match<Event<SampleEvent>>(note => note.Data.Id == "1");
            notifications.Last().Should().Match<Event<SampleEvent>>(note => note.Data.Id == "2");
            notifications.Should().HaveCount(2);
            Trace.WriteLine(DateTime.Now.ToString("T") + " Assert OK.");




            Trace.WriteLine(DateTime.Now.ToString("T") + " Happy End.");


            connecedCount.Should().BeGreaterOrEqualTo(2);
            droppedCount.Should().BeGreaterOrEqualTo(1);

            subscription.Stop();


        }



        [Test]
        [Category("Integration")]
        public async Task ResubscribeToProjectedStream_WhenSubscriberRestarted()
        {

            Trace.WriteLine(DateTime.Now.ToString("T") + " Test");


            var settings = ConfigurationSettings.FromConfig("Full");
            Trace.WriteLine(DateTime.Now.ToString("T") + " Sending Event1.");
            using (var stream = await EventStream.Create(settings, "LinkToProjectionOriginalStream-1"))
            {
                await stream.EmitEventAsync(new SampleEvent { CreatedOn = DateTime.Now, Id = "1" });
            }

            var notifications = new List<Event<SampleEvent>>();


            // ReSharper disable NotAccessedVariable
            int droppedCount = 0;
            int connecedCount = 0;
            // ReSharper restore NotAccessedVariable


            await Projection.Enable(settings, SystemProjection.By_Category);
            await Projection.WithSettings(settings)
                      .Contnuous()
                      .Enabled()
                      .WithName("LinkToProjection")
                      .WithFile("Projections\\LinkToSampleProjection.js")
                      .EmitEnabled()
                      .Create();





            Trace.WriteLine(DateTime.Now.ToString("T") + " Starting Subscription");
            var subscription = Subscription<SampleEvent>.WithSettings(settings)
                                                   .ToStream("ProjectedStream")
                                                   .Durable(new AppConfigCheckpointPersister("ES:SubscriptionCheckpoint"))
                                                   .KeepConnection(RetryCount.NoLimit, TimeSpan.FromSeconds(1))
                                                   .OnConnected(() =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " Connected.");
                                                       connecedCount++;
                                                   })
                                                   .OnDropped((upSubscription, reason, arg3) =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " Dropped. Reason: " + reason);
                                                       droppedCount++;
                                                   })
                                                   .OnEvent(@event =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " OnEvent(s1). Num: " + @event.PositionInTargetStream + ", Event Id: " + @event.Data.Id);
                                                       notifications.Add(@event);
                                                   })
                                                   .OnLiveProcessingStarted(upSubscription => Trace.WriteLine(DateTime.Now.ToString("T") + " Live processing started"))
                                                   .Start();
            await Task.Delay(3000);

            Trace.WriteLine(DateTime.Now.ToString("T") + " Stopping subscription");
            subscription.Stop();


            Trace.WriteLine(DateTime.Now.ToString("T") + " Asserting for 1 event");
            notifications.Single().Should().Match<Event<SampleEvent>>(note => note.Data.Id == "1");
            Trace.WriteLine(DateTime.Now.ToString("T") + " Assert OK.");

            await Task.Delay(2000);

            Trace.WriteLine(DateTime.Now.ToString("T") + " Re-starting subscription");

            var subscription2 = Subscription<SampleEvent>.WithSettings(settings)
                                                   .ToStream("ProjectedStream")
                                                   .Durable(new AppConfigCheckpointPersister("ES:SubscriptionCheckpoint"))
                                                   .KeepConnection(5, TimeSpan.FromSeconds(5))
                                                   .OnConnected(() =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " Connected.");
                                                       connecedCount++;
                                                   })
                                                   .OnDropped((upSubscription, reason, arg3) =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " Dropped. Reason: " + reason);
                                                       droppedCount++;
                                                   })
                                                   .OnEvent(@event =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " OnEvent(s2). Num: " + @event.PositionInTargetStream + ", Event Id: " + @event.Data.Id);
                                                       notifications.Add(@event);
                                                   })
                                                   .OnLiveProcessingStarted(upSubscription => Trace.WriteLine(DateTime.Now.ToString("T") + " Live processing started"))
                                                   .Start();




            Trace.WriteLine(DateTime.Now.ToString("T") + " Sending Event2.");
            using (var stream = await EventStream.Create(settings, "LinkToProjectionOriginalStream-2"))
            {
                await stream.EmitEventAsync(new SampleEvent { CreatedOn = DateTime.Now, Id = "2" });
            }

            await Task.Delay(5000);

            Trace.WriteLine(DateTime.Now.ToString("T") + " Asserting for 2 events");
            notifications.First().Should().Match<Event<SampleEvent>>(note => note.Data.Id == "1");
            notifications.Last().Should().Match<Event<SampleEvent>>(note => note.Data.Id == "2");
            notifications.Should().HaveCount(2);
            Trace.WriteLine(DateTime.Now.ToString("T") + " Assert OK.");


            connecedCount.Should().BeGreaterOrEqualTo(2);
            subscription2.Stop();

            Trace.WriteLine(DateTime.Now.ToString("T") + " Happy End.");


        }



        [Test]
        [Category("Integration")]
        public async Task StopCallingCallbacks_WhenStopped()
        {

            Trace.WriteLine(DateTime.Now.ToString("T") + " Test");


            var settings = ConfigurationSettings.FromConfig("Full");
            Trace.WriteLine(DateTime.Now.ToString("T") + " Sending Event1.");
            using (var stream = await EventStream.Create(settings, "SubscriptionTestStream"))
            {
                await stream.EmitEventAsync("Event1");
            }

            var notifications = new List<Event<String>>();


            // ReSharper disable NotAccessedVariable
            int droppedCount = 0;
            int connecedCount = 0;
            // ReSharper restore NotAccessedVariable

            Trace.WriteLine(DateTime.Now.ToString("T") + " Starting Subscription");
            var subscription = Subscription<String>.WithSettings(settings)
                                                   .ToStream("SubscriptionTestStream")
                                                   .Durable(new AppConfigCheckpointPersister("ES:SubscriptionCheckpoint"))
                                                   .KeepConnection(RetryCount.NoLimit, TimeSpan.FromSeconds(1))
                                                   .OnConnected(() =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " Connected.");
                                                       connecedCount++;
                                                   })
                                                   .OnDropped((upSubscription, reason, arg3) =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " Dropped. Reason: " + reason);
                                                       droppedCount++;
                                                   })
                                                   .OnEvent(@event =>
                                                   {
                                                       Trace.WriteLine(DateTime.Now.ToString("T") + " OnEvent. Num: " + @event.PositionInTargetStream);
                                                       notifications.Add(@event);
                                                   })
                                                   .OnLiveProcessingStarted(upSubscription => Trace.WriteLine(DateTime.Now.ToString("T") + " Live processing started"))
                                                   .Start();



            await Task.Delay(5000);


            Trace.WriteLine(DateTime.Now.ToString("T") + " Stopping subscriber.");
            subscription.Stop();



            Trace.WriteLine(DateTime.Now.ToString("T") + " Asserting for 1 event");
            notifications.Single().Should().Match<Event<String>>(note => note.Data == "Event1");
            notifications.Should().HaveCount(1);
            Trace.WriteLine(DateTime.Now.ToString("T") + " Assert OK.");



            Trace.WriteLine(DateTime.Now.ToString("T") + " Sending Event2.");
            using (var stream = await EventStream.Create(settings, "SubscriptionTestStream"))
            {
                await stream.EmitEventAsync("Event1");
                await stream.EmitEventAsync("Event2");
            }

            await Task.Delay(5000);

            Trace.WriteLine(DateTime.Now.ToString("T") + " Asserting for 1 events");
            notifications.First().Should().Match<Event<String>>(note => note.Data == "Event1");
            notifications.Should().HaveCount(1);
            Trace.WriteLine(DateTime.Now.ToString("T") + " Assert OK.");


            Trace.WriteLine(DateTime.Now.ToString("T") + " Happy End.");




        }



        private void ResetSubscriptionCheckpoint()
        {
            ConfigurationManager.AppSettings["ES:SubscriptionCheckpoint"] = "-1";
            var config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            config.AppSettings.Settings["ES:SubscriptionCheckpoint"].Value = "-1";
            config.Save(ConfigurationSaveMode.Modified);
        }
    }
}