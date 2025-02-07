/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests.Dataload
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Datastream;
    using NUnit.Framework;

    /// <summary>
    /// Data streamer tests.
    /// </summary>
    public sealed class DataStreamerTest
    {
        /** Cache name. */
        private const string CacheName = "partitioned";

        /** Node. */
        private IIgnite _grid;

        /** Node 2. */
        private IIgnite _grid2;

        /** Cache. */
        private ICache<int, int?> _cache;

        /// <summary>
        /// Initialization routine.
        /// </summary>
        [TestFixtureSetUp]
        public void InitClient()
        {
            _grid = Ignition.Start(TestUtils.GetTestConfiguration());

            _grid2 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = "grid1"
            });

            _cache = _grid.CreateCache<int, int?>(CacheName);
        }

        /// <summary>
        /// Fixture teardown.
        /// </summary>
        [TestFixtureTearDown]
        public void StopGrids()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        ///
        /// </summary>
        [SetUp]
        public void BeforeTest()
        {
            Console.WriteLine("Test started: " + TestContext.CurrentContext.Test.Name);

            for (int i = 0; i < 100; i++)
                _cache.Remove(i);
        }

        [TearDown]
        public void AfterTest()
        {
            TestUtils.AssertHandleRegistryIsEmpty(1000, _grid);
        }

        /// <summary>
        /// Test data streamer property configuration. Ensures that at least no exceptions are thrown.
        /// </summary>
        [Test]
        public void TestPropertyPropagation()
        {
            using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                Assert.AreEqual(CacheName, ldr.CacheName);

                Assert.AreEqual(TimeSpan.Zero, ldr.AutoFlushInterval);
                ldr.AutoFlushInterval = TimeSpan.FromMinutes(5);
                Assert.AreEqual(5, ldr.AutoFlushInterval.TotalMinutes);

#pragma warning disable 618 // Type or member is obsolete
                Assert.AreEqual(5 * 60 * 1000, ldr.AutoFlushFrequency);
                ldr.AutoFlushFrequency = 9000;
                Assert.AreEqual(9000, ldr.AutoFlushFrequency);
                Assert.AreEqual(9, ldr.AutoFlushInterval.TotalSeconds);
#pragma warning restore 618 // Type or member is obsolete

                Assert.IsFalse(ldr.AllowOverwrite);
                ldr.AllowOverwrite = true;
                Assert.IsTrue(ldr.AllowOverwrite);
                ldr.AllowOverwrite = false;
                Assert.IsFalse(ldr.AllowOverwrite);

                Assert.IsFalse(ldr.SkipStore);
                ldr.SkipStore = true;
                Assert.IsTrue(ldr.SkipStore);
                ldr.SkipStore = false;
                Assert.IsFalse(ldr.SkipStore);

                Assert.AreEqual(DataStreamerDefaults.DefaultPerNodeBufferSize, ldr.PerNodeBufferSize);
                ldr.PerNodeBufferSize = 1;
                Assert.AreEqual(1, ldr.PerNodeBufferSize);
                ldr.PerNodeBufferSize = 2;
                Assert.AreEqual(2, ldr.PerNodeBufferSize);

                Assert.AreEqual(DataStreamerDefaults.DefaultPerThreadBufferSize, ldr.PerThreadBufferSize);
                ldr.PerThreadBufferSize = 1;
                Assert.AreEqual(1, ldr.PerThreadBufferSize);
                ldr.PerThreadBufferSize = 2;
                Assert.AreEqual(2, ldr.PerThreadBufferSize);

                Assert.AreEqual(0, ldr.PerNodeParallelOperations);
                var ops = DataStreamerDefaults.DefaultParallelOperationsMultiplier *
                          IgniteConfiguration.DefaultThreadPoolSize;
                ldr.PerNodeParallelOperations = ops;
                Assert.AreEqual(ops, ldr.PerNodeParallelOperations);
                ldr.PerNodeParallelOperations = 2;
                Assert.AreEqual(2, ldr.PerNodeParallelOperations);

                Assert.AreEqual(DataStreamerDefaults.DefaultTimeout, ldr.Timeout);
                ldr.Timeout = TimeSpan.MaxValue;
                Assert.AreEqual(TimeSpan.MaxValue, ldr.Timeout);
                ldr.Timeout = TimeSpan.FromSeconds(1.5);
                Assert.AreEqual(1.5, ldr.Timeout.TotalSeconds);
            }
        }

        /// <summary>
        /// Tests removal without <see cref="IDataStreamer{TK,TV}.AllowOverwrite"/>.
        /// </summary>
        [Test]
        public void TestRemoveNoOverwrite()
        {
            _cache.Put(1, 1);

            using (var ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                ldr.Remove(1);
            }

            Assert.IsTrue(_cache.ContainsKey(1));
        }

        /// <summary>
        /// Test data add/remove.
        /// </summary>
        [Test]
        public void TestAddRemove()
        {
            IDataStreamer<int, int> ldr;

            using (ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                Assert.IsFalse(ldr.Task.IsCompleted);

                ldr.AllowOverwrite = true;

                // Additions.
                var task = ldr.GetCurrentBatchTask();
                ldr.Add(1, 1);
                ldr.Flush();
                Assert.AreEqual(1, _cache.Get(1));
                Assert.IsTrue(task.IsCompleted);
                Assert.IsFalse(ldr.Task.IsCompleted);

                task = ldr.GetCurrentBatchTask();
                ldr.Add(new KeyValuePair<int, int>(2, 2));
                ldr.Flush();
                Assert.AreEqual(2, _cache.Get(2));
                Assert.IsTrue(task.IsCompleted);

                task = ldr.GetCurrentBatchTask();
                ldr.Add(new [] { new KeyValuePair<int, int>(3, 3), new KeyValuePair<int, int>(4, 4) });
                ldr.Flush();
                Assert.AreEqual(3, _cache.Get(3));
                Assert.AreEqual(4, _cache.Get(4));
                Assert.IsTrue(task.IsCompleted);

                // Removal.
                task = ldr.GetCurrentBatchTask();
                ldr.Remove(1);
                ldr.Flush();
                Assert.IsFalse(_cache.ContainsKey(1));
                Assert.IsTrue(task.IsCompleted);

                // Mixed.
                ldr.Add(5, 5);
                ldr.Remove(2);
                ldr.Add(new KeyValuePair<int, int>(7, 7));
                ldr.Add(6, 6);
                ldr.Remove(4);
                ldr.Add(new List<KeyValuePair<int, int>> { new KeyValuePair<int, int>(9, 9), new KeyValuePair<int, int>(10, 10) });
                ldr.Add(new KeyValuePair<int, int>(8, 8));
                ldr.Remove(3);
                ldr.Add(new List<KeyValuePair<int, int>> { new KeyValuePair<int, int>(11, 11), new KeyValuePair<int, int>(12, 12) });

                ldr.Flush();

                for (int i = 2; i < 5; i++)
                    Assert.IsFalse(_cache.ContainsKey(i));

                for (int i = 5; i < 13; i++)
                    Assert.AreEqual(i, _cache.Get(i));
            }

            Assert.IsTrue(ldr.Task.Wait(5000));
        }

        /// <summary>
        /// Test data add/remove.
        /// </summary>
        [Test]
        public void TestAddRemoveObsolete()
        {
#pragma warning disable 618 // Type or member is obsolete
            IDataStreamer<int, int> ldr;

            using (ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                Assert.IsFalse(ldr.Task.IsCompleted);

                ldr.AllowOverwrite = true;

                // Additions.
                var task = ldr.AddData(1, 1);
                ldr.Flush();
                Assert.AreEqual(1, _cache.Get(1));
                Assert.IsTrue(task.IsCompleted);
                Assert.IsFalse(ldr.Task.IsCompleted);

                task = ldr.AddData(new KeyValuePair<int, int>(2, 2));
                ldr.Flush();
                Assert.AreEqual(2, _cache.Get(2));
                Assert.IsTrue(task.IsCompleted);

                task = ldr.AddData(new [] { new KeyValuePair<int, int>(3, 3), new KeyValuePair<int, int>(4, 4) });
                ldr.Flush();
                Assert.AreEqual(3, _cache.Get(3));
                Assert.AreEqual(4, _cache.Get(4));
                Assert.IsTrue(task.IsCompleted);

                // Removal.
                task = ldr.RemoveData(1);
                ldr.Flush();
                Assert.IsFalse(_cache.ContainsKey(1));
                Assert.IsTrue(task.IsCompleted);

                // Mixed.
                ldr.AddData(5, 5);
                ldr.RemoveData(2);
                ldr.AddData(new KeyValuePair<int, int>(7, 7));
                ldr.AddData(6, 6);
                ldr.RemoveData(4);
                ldr.AddData(new List<KeyValuePair<int, int>> { new KeyValuePair<int, int>(9, 9), new KeyValuePair<int, int>(10, 10) });
                ldr.AddData(new KeyValuePair<int, int>(8, 8));
                ldr.RemoveData(3);
                ldr.AddData(new List<KeyValuePair<int, int>> { new KeyValuePair<int, int>(11, 11), new KeyValuePair<int, int>(12, 12) });

                ldr.Flush();

                for (int i = 2; i < 5; i++)
                    Assert.IsFalse(_cache.ContainsKey(i));

                for (int i = 5; i < 13; i++)
                    Assert.AreEqual(i, _cache.Get(i));
            }

            Assert.IsTrue(ldr.Task.Wait(5000));
#pragma warning restore 618 // Type or member is obsolete
        }

        /// <summary>
        /// Tests object graphs with loops.
        /// </summary>
        [Test]
        public void TestObjectGraphs()
        {
            var obj1 = new Container();
            var obj2 = new Container();
            var obj3 = new Container();
            var obj4 = new Container();

            obj1.Inner = obj2;
            obj2.Inner = obj1;
            obj3.Inner = obj1;
            obj4.Inner = new Container();

            using (var ldr = _grid.GetDataStreamer<int, Container>(CacheName))
            {
                ldr.AllowOverwrite = true;

                ldr.Add(1, obj1);
                ldr.Add(2, obj2);
                ldr.Add(3, obj3);
                ldr.Add(4, obj4);
            }

            var cache = _grid.GetCache<int, Container>(CacheName);

            var res = cache[1];
            Assert.AreEqual(res, res.Inner.Inner);

            Assert.IsNotNull(cache[2].Inner);
            Assert.IsNotNull(cache[2].Inner.Inner);
            Assert.IsNotNull(cache[3].Inner);
            Assert.IsNotNull(cache[3].Inner.Inner);

            Assert.IsNotNull(cache[4].Inner);
            Assert.IsNull(cache[4].Inner.Inner);
        }

        /// <summary>
        /// Test "tryFlush".
        /// </summary>
        [Test]
        public void TestTryFlushObsolete()
        {
#pragma warning disable 618 // Type or member is obsolete
            using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                var fut = ldr.AddData(1, 1);

                ldr.TryFlush();

                fut.Wait();

                Assert.AreEqual(1, _cache.Get(1));
            }
#pragma warning restore 618 // Type or member is obsolete
        }

        /// <summary>
        /// Test FlushAsync.
        /// </summary>
        [Test]
        public void TestFlushAsync()
        {
            using (var ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                ldr.Add(1, 1);

                ldr.FlushAsync().Wait();

                Assert.AreEqual(1, _cache.Get(1));
            }
        }

        /// <summary>
        /// Test buffer size adjustments.
        /// </summary>
        [Test]
        public void TestBufferSize()
        {
            using (var ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                const int timeout = 5000;

                var part1 = GetPrimaryPartitionKeys(_grid, 4);
                var part2 = GetPrimaryPartitionKeys(_grid2, 4);

                ldr.Add(part1[0], part1[0]);

                var task = ldr.GetCurrentBatchTask();

                Thread.Sleep(100);

                Assert.IsFalse(task.IsCompleted);

                ldr.PerNodeBufferSize = 2;
                ldr.PerThreadBufferSize = 1;

                ldr.Add(part2[0], part2[0]);
                ldr.Add(part1[1], part1[1]);
                ldr.Add(part2[1], part2[1]);
                Assert.IsTrue(task.Wait(timeout));

                Assert.AreEqual(part1[0], _cache.Get(part1[0]));
                Assert.AreEqual(part1[1], _cache.Get(part1[1]));
                Assert.AreEqual(part2[0], _cache.Get(part2[0]));
                Assert.AreEqual(part2[1], _cache.Get(part2[1]));

                var task2 = ldr.GetCurrentBatchTask();

                ldr.Add(new[]
                {
                    new KeyValuePair<int, int>(part1[2], part1[2]),
                    new KeyValuePair<int, int>(part1[3], part1[3]),
                    new KeyValuePair<int, int>(part2[2], part2[2]),
                    new KeyValuePair<int, int>(part2[3], part2[3])
                });

                Assert.IsTrue(task2.Wait(timeout));

                Assert.AreEqual(part1[2], _cache.Get(part1[2]));
                Assert.AreEqual(part1[3], _cache.Get(part1[3]));
                Assert.AreEqual(part2[2], _cache.Get(part2[2]));
                Assert.AreEqual(part2[3], _cache.Get(part2[3]));
            }
        }

        /// <summary>
        /// Gets the primary partition keys.
        /// </summary>
        private static int[] GetPrimaryPartitionKeys(IIgnite ignite, int count)
        {
            var affinity = ignite.GetAffinity(CacheName);

            var localNode = ignite.GetCluster().GetLocalNode();

            var part = affinity.GetPrimaryPartitions(localNode).First();

            return Enumerable.Range(0, int.MaxValue)
                .Where(k => affinity.GetPartition(k) == part)
                .Take(count)
                .ToArray();
        }

        /// <summary>
        /// Test close.
        /// </summary>
        [Test]
        public void TestClose()
        {
            using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                var fut = ldr.GetCurrentBatchTask();
                ldr.Add(1, 1);

                ldr.Close(false);

                Assert.IsTrue(fut.Wait(5000));
                Assert.AreEqual(1, _cache.Get(1));
            }
        }

        /// <summary>
        /// Test close with cancellation.
        /// </summary>
        [Test]
        public void TestCancel()
        {
            using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                var fut = ldr.GetCurrentBatchTask();
                ldr.Add(1, 1);

                ldr.Close(true);

                Assert.IsTrue(fut.Wait(5000));
                Assert.IsFalse(_cache.ContainsKey(1));
            }
        }

        /// <summary>
        /// Tests that streamer gets collected when there are no references to it.
        /// </summary>
        [Test]
        public void TestFinalizer()
        {
            // Create streamer reference in a different thread to defeat Debug mode quirks.
            var streamerRef = Task.Factory.StartNew
                (() => new WeakReference(_grid.GetDataStreamer<int, int>(CacheName))).Result;

            GC.Collect();
            GC.WaitForPendingFinalizers();

            Assert.IsNull(streamerRef.Target);
        }

        /// <summary>
        /// Test auto-flush feature.
        /// </summary>
        [Test]
        public void TestAutoFlushObsolete()
        {
#pragma warning disable 618 // Type or member is obsolete
            using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                // Test auto flush turning on.
                var fut = ldr.AddData(1, 1);
                Thread.Sleep(100);
                Assert.IsFalse(fut.IsCompleted);
                ldr.AutoFlushFrequency = 1000;
                fut.Wait();

                // Test forced flush after frequency change.
                fut = ldr.AddData(2, 2);
                ldr.AutoFlushFrequency = long.MaxValue;
                fut.Wait();

                // Test another forced flush after frequency change.
                fut = ldr.AddData(3, 3);
                ldr.AutoFlushFrequency = 1000;
                fut.Wait();

                // Test flush before stop.
                fut = ldr.AddData(4, 4);
                ldr.AutoFlushFrequency = 0;
                fut.Wait();

                // Test flush after second turn on.
                fut = ldr.AddData(5, 5);
                ldr.AutoFlushFrequency = 1000;
                fut.Wait();

                Assert.AreEqual(1, _cache.Get(1));
                Assert.AreEqual(2, _cache.Get(2));
                Assert.AreEqual(3, _cache.Get(3));
                Assert.AreEqual(4, _cache.Get(4));
                Assert.AreEqual(5, _cache.Get(5));
            }
#pragma warning restore 618 // Type or member is obsolete
        }

        /// <summary>
        /// Test auto-flush feature.
        /// </summary>
        [Test]
        public void TestAutoFlush()
        {
            using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                // Test auto flush turning on.
                var fut = ldr.GetCurrentBatchTask();
                ldr.Add(1, 1);
                Thread.Sleep(100);
                Assert.IsFalse(fut.IsCompleted);
                ldr.AutoFlushInterval = TimeSpan.FromSeconds(1);
                fut.Wait();

                // Test forced flush after frequency change.
                fut = ldr.GetCurrentBatchTask();
                ldr.Add(2, 2);
                ldr.AutoFlushInterval = TimeSpan.MaxValue;
                fut.Wait();

                // Test another forced flush after frequency change.
                fut = ldr.GetCurrentBatchTask();
                ldr.Add(3, 3);
                ldr.AutoFlushInterval = TimeSpan.FromSeconds(1);
                fut.Wait();

                // Test flush before stop.
                fut = ldr.GetCurrentBatchTask();
                ldr.Add(4, 4);
                ldr.AutoFlushInterval = TimeSpan.Zero;
                fut.Wait();

                // Test flush after second turn on.
                fut = ldr.GetCurrentBatchTask();
                ldr.Add(5, 5);
                ldr.AutoFlushInterval = TimeSpan.FromSeconds(1);
                fut.Wait();

                Assert.AreEqual(1, _cache.Get(1));
                Assert.AreEqual(2, _cache.Get(2));
                Assert.AreEqual(3, _cache.Get(3));
                Assert.AreEqual(4, _cache.Get(4));
                Assert.AreEqual(5, _cache.Get(5));
            }
        }

        /// <summary>
        /// Test multithreaded behavior.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestMultithreaded()
        {
            int entriesPerThread = 100000;
            int threadCnt = 8;

            for (int i = 0; i < 5; i++)
            {
                _cache.Clear();

                Assert.AreEqual(0, _cache.GetSize());

                Stopwatch watch = new Stopwatch();

                watch.Start();

                using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
                {
                    ldr.PerNodeBufferSize = 1024;

                    int ctr = 0;

                    TestUtils.RunMultiThreaded(() =>
                    {
                        int threadIdx = Interlocked.Increment(ref ctr);

                        int startIdx = (threadIdx - 1) * entriesPerThread;
                        int endIdx = startIdx + entriesPerThread;

                        for (int j = startIdx; j < endIdx; j++)
                        {
                            // ReSharper disable once AccessToDisposedClosure
                            ldr.Add(j, j);

                            if (j % 100000 == 0)
                                Console.WriteLine("Put [thread=" + threadIdx + ", cnt=" + j  + ']');
                        }
                    }, threadCnt);
                }

                Console.WriteLine("Iteration " + i + ": " + watch.ElapsedMilliseconds);

                watch.Reset();

                for (int j = 0; j < threadCnt * entriesPerThread; j++)
                    Assert.AreEqual(j, j);
            }
        }

        /// <summary>
        /// Tests custom receiver.
        /// </summary>
        [Test]
        public void TestStreamReceiver()
        {
            TestStreamReceiver(new StreamReceiverBinarizable());
            TestStreamReceiver(new StreamReceiverSerializable());
        }

        /// <summary>
        /// Tests StreamVisitor.
        /// </summary>
        [Test]
        public void TestStreamVisitor()
        {
#if !NETCOREAPP // Serializing delegates is not supported on this platform.
            TestStreamReceiver(new StreamVisitor<int, int>((c, e) => c.Put(e.Key, e.Value + 1)));
#endif
        }

        /// <summary>
        /// Tests StreamTransformer.
        /// </summary>
        [Test]
        public void TestStreamTransformer()
        {
            TestStreamReceiver(new StreamTransformer<int, int, int, int>(new EntryProcessorSerializable()));
            TestStreamReceiver(new StreamTransformer<int, int, int, int>(new EntryProcessorBinarizable()));
        }

        [Test]
        public void TestStreamTransformerIsInvokedForDuplicateKeys()
        {
            var cache = _grid.GetOrCreateCache<string, long>("c");

            using (var streamer = _grid.GetDataStreamer<string, long>(cache.Name))
            {
                streamer.AllowOverwrite = true;
                streamer.Receiver = new StreamTransformer<string, long, object, object>(new CountingEntryProcessor());

                var words = Enumerable.Repeat("a", 3).Concat(Enumerable.Repeat("b", 2));
                foreach (var word in words)
                {
                    streamer.Add(word, 1L);
                }
            }

            Assert.AreEqual(3, cache.Get("a"));
            Assert.AreEqual(2, cache.Get("b"));
        }

        /// <summary>
        /// Tests specified receiver.
        /// </summary>
        private void TestStreamReceiver(IStreamReceiver<int, int> receiver)
        {
            using (var ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                ldr.AllowOverwrite = true;

                ldr.Receiver = new StreamReceiverBinarizable();

                ldr.Receiver = receiver;  // check double assignment

                Assert.AreEqual(ldr.Receiver, receiver);

                for (var i = 0; i < 100; i++)
                    ldr.Add(i, i);

                ldr.Flush();

                for (var i = 0; i < 100; i++)
                    Assert.AreEqual(i + 1, _cache.Get(i));
            }
        }

        /// <summary>
        /// Tests the stream receiver in keepBinary mode.
        /// </summary>
        [Test]
        public void TestStreamReceiverKeepBinary()
        {
            // ReSharper disable once LocalVariableHidesMember
            var cache = _grid.GetCache<int, BinarizableEntry>(CacheName);

            using (var ldr0 = _grid.GetDataStreamer<int, int>(CacheName))
            using (var ldr = ldr0.WithKeepBinary<int, IBinaryObject>())
            {
                ldr.Receiver = new StreamReceiverKeepBinary();

                ldr.AllowOverwrite = true;

                for (var i = 0; i < 100; i++)
                    ldr.Add(i, _grid.GetBinary().ToBinary<IBinaryObject>(new BinarizableEntry {Val = i}));

                ldr.Flush();

                for (var i = 0; i < 100; i++)
                    Assert.AreEqual(i + 1, cache.Get(i).Val);

                // Repeating WithKeepBinary call: valid args.
                Assert.AreSame(ldr, ldr.WithKeepBinary<int, IBinaryObject>());

                // Invalid type args.
                var ex = Assert.Throws<InvalidOperationException>(() => ldr.WithKeepBinary<string, IBinaryObject>());

                Assert.AreEqual(
                    "Can't change type of binary streamer. WithKeepBinary has been called on an instance of " +
                    "binary streamer with incompatible generic arguments.", ex.Message);
            }
        }

        /// <summary>
        /// Streamer test with destroyed cache.
        /// </summary>
        [Test]
        public void TestDestroyCache()
        {
            var cache = _grid.CreateCache<int, int>(TestUtils.TestName);

            var streamer = _grid.GetDataStreamer<int, int>(cache.Name);

            streamer.Add(1, 2);
            streamer.FlushAsync().Wait();

            _grid.DestroyCache(cache.Name);

            streamer.Add(2, 3);

            var ex = Assert.Throws<AggregateException>(() => streamer.Flush()).GetBaseException();

            Assert.IsNotNull(ex);

            Assert.AreEqual("class org.apache.ignite.IgniteCheckedException: DataStreamer data loading failed.",
                ex.Message);

            Assert.Throws<CacheException>(() => streamer.Close(true));
        }

        /// <summary>
        /// Streamer test with destroyed cache.
        /// </summary>
        [Test]
        public void TestDestroyCacheObsolete()
        {
#pragma warning disable 618 // Type or member is obsolete
            var cache = _grid.CreateCache<int, int>(TestUtils.TestName);

            var streamer = _grid.GetDataStreamer<int, int>(cache.Name);

            var task = streamer.AddData(1, 2);
            streamer.Flush();
            task.Wait();

            _grid.DestroyCache(cache.Name);

            streamer.AddData(2, 3);

            var ex = Assert.Throws<AggregateException>(() => streamer.Flush()).GetBaseException();

            Assert.IsNotNull(ex);

            Assert.AreEqual("class org.apache.ignite.IgniteCheckedException: DataStreamer data loading failed.",
                ex.Message);

            Assert.Throws<CacheException>(() => streamer.Close(true));
#pragma warning restore 618 // Type or member is obsolete
        }


        /// <summary>
        /// Tests that streaming binary objects with a thin client results in those objects being
        /// available through SQL in the cache's table.
        /// </summary>
        [Test]
        public void TestBinaryStreamerCreatesSqlRecord()
        {
            var cacheCfg = new CacheConfiguration
            {
                Name = "TestBinaryStreamerCreatesSqlRecord",
                SqlSchema = "persons",
                QueryEntities = new[]
                {
                    new QueryEntity
                    {
                        ValueTypeName = "Person",
                        Fields = new List<QueryField>
                        {
                            new QueryField
                            {
                                Name = "Name",
                                FieldType = typeof(string),
                            },
                            new QueryField
                            {
                                Name = "Age",
                                FieldType = typeof(int)
                            }
                        }
                    }
                }
            };

            var cacheClientBinary = _grid.GetOrCreateCache<int, IBinaryObject>(cacheCfg)
                .WithKeepBinary<int, IBinaryObject>();

            // Prepare a binary object.
            var jane = _grid.GetBinary().GetBuilder("Person")
                .SetStringField("Name", "Jane")
                .SetIntField("Age", 43)
                .Build();

            const int key = 1;

            // Stream the binary object to the server.
            using (var streamer = _grid.GetDataStreamer<int, IBinaryObject>(cacheCfg.Name))
            {
                streamer.Add(key, jane);
                streamer.Flush();
            }

            // Check that SQL works.
            var query = new SqlFieldsQuery("SELECT Name, Age FROM \"PERSONS\".PERSON");
            var fullResultAfterClientStreamer = cacheClientBinary.Query(query).GetAll();
            Assert.IsNotNull(fullResultAfterClientStreamer);
            Assert.AreEqual(1, fullResultAfterClientStreamer.Count);
            Assert.AreEqual("Jane", fullResultAfterClientStreamer[0][0]);
            Assert.AreEqual(43, fullResultAfterClientStreamer[0][1]);
        }

#if NETCOREAPP
        /// <summary>
        /// Tests async streamer usage.
        /// Using async cache and streamer operations within the streamer means that we end up on different threads.
        /// Streamer is thread-safe and is expected to handle this well.
        /// </summary>
        [Test]
        public async Task TestStreamerAsyncAwait()
        {
            using (var ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                ldr.AllowOverwrite = true;

                ldr.Add(Enumerable.Range(1, 500).ToDictionary(x => x, x => -x));

                Assert.IsFalse(await _cache.ContainsKeysAsync(new[] {1, 2}));

                var flushTask = ldr.FlushAsync();
                Assert.IsFalse(flushTask.IsCompleted);
                await flushTask;

                Assert.AreEqual(-1, await _cache.GetAsync(1));
                Assert.AreEqual(-2, await _cache.GetAsync(2));

                // Remove.
                var batchTask = ldr.GetCurrentBatchTask();
                Assert.IsFalse(batchTask.IsCompleted);
                Assert.IsFalse(batchTask.IsFaulted);

                ldr.Remove(1);
                var flushTask2 = ldr.FlushAsync();

                Assert.AreSame(batchTask, flushTask2);
                await flushTask2;

                Assert.IsTrue(batchTask.IsCompleted);
                Assert.IsFalse(await _cache.ContainsKeyAsync(1));

                // Empty buffer flush is allowed.
                await ldr.FlushAsync();
                await ldr.FlushAsync();
            }
        }
#endif

        /// <summary>
        /// Test binarizable receiver.
        /// </summary>
        private class StreamReceiverBinarizable : IStreamReceiver<int, int>
        {
            /** <inheritdoc /> */
            public void Receive(ICache<int, int> cache, ICollection<ICacheEntry<int, int>> entries)
            {
                cache.PutAll(entries.ToDictionary(x => x.Key, x => x.Value + 1));
            }
        }

        /// <summary>
        /// Test binary receiver.
        /// </summary>
        [Serializable]
        private class StreamReceiverKeepBinary : IStreamReceiver<int, IBinaryObject>
        {
            /** <inheritdoc /> */
            public void Receive(ICache<int, IBinaryObject> cache, ICollection<ICacheEntry<int, IBinaryObject>> entries)
            {
                var binary = cache.Ignite.GetBinary();

                cache.PutAll(entries.ToDictionary(x => x.Key, x =>
                    binary.ToBinary<IBinaryObject>(new BinarizableEntry
                    {
                        Val = x.Value.Deserialize<BinarizableEntry>().Val + 1
                    })));
            }
        }

        /// <summary>
        /// Test serializable receiver.
        /// </summary>
        [Serializable]
        private class StreamReceiverSerializable : IStreamReceiver<int, int>
        {
            /** <inheritdoc /> */
            public void Receive(ICache<int, int> cache, ICollection<ICacheEntry<int, int>> entries)
            {
                cache.PutAll(entries.ToDictionary(x => x.Key, x => x.Value + 1));
            }
        }

        /// <summary>
        /// Test entry processor.
        /// </summary>
        [Serializable]
        private class EntryProcessorSerializable : ICacheEntryProcessor<int, int, int, int>
        {
            /** <inheritdoc /> */
            public int Process(IMutableCacheEntry<int, int> entry, int arg)
            {
                entry.Value = entry.Key + 1;

                return 0;
            }
        }

        /// <summary>
        /// Test entry processor.
        /// </summary>
        private class EntryProcessorBinarizable : ICacheEntryProcessor<int, int, int, int>, IBinarizable
        {
            /** <inheritdoc /> */
            public int Process(IMutableCacheEntry<int, int> entry, int arg)
            {
                entry.Value = entry.Key + 1;

                return 0;
            }

            /** <inheritdoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                // No-op.
            }

            /** <inheritdoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                // No-op.
            }
        }

        /// <summary>
        /// Binarizable entry.
        /// </summary>
        private class BinarizableEntry
        {
            public int Val { get; set; }
        }

        /// <summary>
        /// Container class.
        /// </summary>
        private class Container
        {
            public Container Inner;
        }

        private class CountingEntryProcessor : ICacheEntryProcessor<string, long, object, object>
        {
            public object Process(IMutableCacheEntry<string, long> e, object arg)
            {
                e.Value++;

                return null;
            }
        }
    }
}
