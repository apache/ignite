/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests.EntityFramework
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.Entity;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.EntityFramework;
    using NUnit.Framework;

    /// <summary>
    /// Integration test with temporary SQL CE database.
    /// </summary>
    public class EntityFrameworkSecondLevelCacheTest : IEventListener<CacheEvent>
    {
        /** */
        private static readonly string TempFile = Path.GetTempFileName();
        
        /** */
        private static readonly string ConnectionString = "Datasource = " + TempFile;

        /** */
        private readonly ConcurrentStack<CacheEvent> _events = new ConcurrentStack<CacheEvent>();

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            // Start Ignite.
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());

            // Subscribe to cache events.
            var events = ignite.GetEvents();
            events.EnableLocal(EventType.CacheObjectPut, EventType.CacheObjectRead, EventType.CacheObjectExpired);
            events.LocalListen(this, EventType.CacheAll);

            // Create SQL CE database in a temp file.
            using (var context = new BloggingContext(ConnectionString))
            {
                File.Delete(TempFile);
                context.Database.Create();
            }
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
            File.Delete(TempFile);
        }

        [Test]
        public void Test()
        {
            using (var context = new BloggingContext(ConnectionString))
            {
                //context.Database.Log = s => Debug.WriteLine(s);

                Assert.IsEmpty(context.Blogs);
                Assert.IsEmpty(context.Posts);

                // Each query generates 3 events: get, put key-val, put dependency
                Assert.AreEqual(6, _events.Count);

                context.Blogs.Add(new Blog
                {
                    BlogId = 1,
                    Name = "Foo",
                    Posts = new List<Post>
                    {
                        new Post {Title = "My First Post", Content = "Hello World!"}
                    }
                });

                Assert.AreEqual(2, context.SaveChanges());

                // TODO: Check invalidation events
                // INSERT should not be cached

                Assert.AreEqual(1, context.Posts.Where(x => x.Title.StartsWith("My")).ToArray().Length);

                Assert.AreEqual(3, _events.Count);
            }
        }

        private class MyDbConfiguration : IgniteDbConfiguration
        {
            public MyDbConfiguration() : base(Ignition.GetIgnite(), null, null)
            {
            }
        }


        [DbConfigurationType(typeof(MyDbConfiguration))]
        private class BloggingContext : DbContext
        {
            public BloggingContext(string nameOrConnectionString) : base(nameOrConnectionString)
            {
                // No-op.
            }


            public virtual DbSet<Blog> Blogs { get; set; }
            public virtual DbSet<Post> Posts { get; set; }
        }

        private class Blog
        {
            public int BlogId { get; set; }
            public string Name { get; set; }
            public string Url { get; set; }

            public virtual List<Post> Posts { get; set; }
        }

        private class Post
        {
            public int PostId { get; set; }
            public string Title { get; set; }
            public string Content { get; set; }

            public int BlogId { get; set; }
            public virtual Blog Blog { get; set; }

            // TODO: Test all kinds of field types (to test serialization and reader)
        }

        public bool Invoke(CacheEvent evt)
        {
            _events.Push(evt);

            return true;
        }
    }
}
