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

// ReSharper disable UnusedMember.Local
// ReSharper disable UnusedAutoPropertyAccessor.Local
// ReSharper disable ClassWithVirtualMembersNeverInherited.Local
// ReSharper disable UnusedAutoPropertyAccessor.Global
namespace Apache.Ignite.Core.Tests.EntityFramework
{
    using System;
    using System.Collections.Generic;
    using System.Data.Entity;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.EntityFramework;
    using NUnit.Framework;

    /// <summary>
    /// Integration test with temporary SQL CE database.
    /// </summary>
    public class EntityFrameworkCacheTest
    {
        /** */
        private static readonly string TempFile = Path.GetTempFileName();

        /** */
        private static readonly string ConnectionString = "Datasource = " + TempFile;

        /** */
        private ICache<object, object> _cache;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            // Prepare SQL CE support.
            // Database.DefaultConnectionFactory = new SqlCeConnectionFactory("System.Data.SqlServerCe.4.0");

            // Start Ignite.
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());

            // Create SQL CE database in a temp file.
            using (var context = new BloggingContext(ConnectionString))
            {
                File.Delete(TempFile);
                context.Database.Create();
            }

            // Get the cache.
            _cache = ignite.GetCache<object, object>(null);
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

        /// <summary>
        /// Tests the strict strategy.
        /// </summary>
        [Test]
        public void TestStrictStrategy()
        {
            using (var context = new BloggingContext(ConnectionString))
            {
                //context.Database.Log = s => Debug.WriteLine(s);

                Assert.IsEmpty(context.Blogs);
                Assert.IsEmpty(context.Posts);

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

                // Check that query works.
                Assert.AreEqual(1, context.Posts.Where(x => x.Title.StartsWith("My")).ToArray().Length);

                // Add new post to check invalidation.
                context.Posts.Add(new Post {BlogId = 1, Title = "My Second Post", Content = "Foo bar."});
                Assert.AreEqual(1, context.SaveChanges());
                
                Assert.AreEqual(2, _cache.GetSize()); // Only entity set versions are in cache.

                Assert.AreEqual(2, context.Posts.Where(x => x.Title.StartsWith("My")).ToArray().Length);

                Assert.AreEqual(3, _cache.GetSize()); // Cached query added.

                // Delete post.
                context.Posts.Remove(context.Posts.First());
                Assert.AreEqual(1, context.SaveChanges());

                Assert.AreEqual(2, _cache.GetSize()); // Only entity set versions are in cache.
                Assert.AreEqual(1, context.Posts.Where(x => x.Title.StartsWith("My")).ToArray().Length);

                Assert.AreEqual(3, _cache.GetSize()); // Cached query added.

                // Modify post.
                Assert.AreEqual(0, context.Posts.Count(x => x.Title.EndsWith("updated")));

                context.Posts.Single().Title += " - updated";
                Assert.AreEqual(1, context.SaveChanges());

                Assert.AreEqual(2, _cache.GetSize()); // Only entity set versions are in cache.
                Assert.AreEqual(1, context.Posts.Count(x => x.Title.EndsWith("updated")));

                Assert.AreEqual(3, _cache.GetSize()); // Cached query added.
            }
        }

        /// <summary>
        /// Tests transactions.
        /// </summary>
        [Test]
        public void TestTx()
        {
            // TODO: Find out what's called within a TX.
        }

        /// <summary>
        /// Tests the expiration.
        /// </summary>
        [Test]
        public void TestExpiration()
        {
            // TODO: 
        }

        /// <summary>
        /// Tests the cache reader.
        /// </summary>
        [Test]
        public void TestCacheReader()
        {
            // Tests all kinds of entity field types to cover ArrayDbDataReader
            var test = new ArrayReaderTest
            {
                DateTime = DateTime.Today,
                Bool = true,
                Byte = 56,
                String = "z",
                Decimal = (decimal) 5.6,
                Double = 7.8d,
                Float = -4.5f,
                Guid = Guid.NewGuid(),
                ArrayReaderTestId = -8,
                Long = 3,
                Short = 5
            };

            using (var context = new BloggingContext(ConnectionString))
            {
                context.Tests.Add(test);
                context.SaveChanges();
            }

            // Use new context to ensure no first-level caching.
            using (var context = new BloggingContext(ConnectionString))
            {
                // Check default deserialization.
                var test0 = context.Tests.Single(x => x.Bool);
                Assert.AreEqual(test, test0);
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
            public virtual DbSet<ArrayReaderTest> Tests { get; set; }
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
        }

        private class ArrayReaderTest
        {
            public byte Byte { get; set; }
            public short Short { get; set; }
            public int ArrayReaderTestId { get; set; }
            public long Long { get; set; }
            public float Float { get; set; }
            public double Double { get; set; }
            public decimal Decimal { get; set; }
            public bool Bool { get; set; }
            public string String { get; set; }
            public Guid Guid { get; set; }
            public DateTime DateTime { get; set; }

            private bool Equals(ArrayReaderTest other)
            {
                return Byte == other.Byte && Short == other.Short &&
                       ArrayReaderTestId == other.ArrayReaderTestId && Long == other.Long && 
                       Float.Equals(other.Float) && Double.Equals(other.Double) && 
                       Decimal == other.Decimal && Bool == other.Bool && String == other.String && 
                       Guid.Equals(other.Guid) && DateTime.Equals(other.DateTime);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((ArrayReaderTest) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = Byte.GetHashCode();
                    hashCode = (hashCode*397) ^ Short.GetHashCode();
                    hashCode = (hashCode*397) ^ ArrayReaderTestId;
                    hashCode = (hashCode*397) ^ Long.GetHashCode();
                    hashCode = (hashCode*397) ^ Float.GetHashCode();
                    hashCode = (hashCode*397) ^ Double.GetHashCode();
                    hashCode = (hashCode*397) ^ Decimal.GetHashCode();
                    hashCode = (hashCode*397) ^ Bool.GetHashCode();
                    hashCode = (hashCode*397) ^ String.GetHashCode();
                    hashCode = (hashCode*397) ^ Guid.GetHashCode();
                    hashCode = (hashCode*397) ^ DateTime.GetHashCode();
                    return hashCode;
                }
            }
        }
    }
}
