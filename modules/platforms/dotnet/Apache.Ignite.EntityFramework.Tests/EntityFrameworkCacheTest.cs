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
// ReSharper disable VirtualMemberNeverOverridden.Global

namespace Apache.Ignite.EntityFramework.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Entity;
    using System.Data.Entity.Core.EntityClient;
    using System.Data.Entity.Infrastructure;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Transactions;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Tests;
    using Apache.Ignite.EntityFramework;
    using Apache.Ignite.EntityFramework.Impl;
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
        private static readonly DelegateCachingPolicy Policy = new DelegateCachingPolicy();

        /** */
        private ICache<object, object> _cache;

        /** */
        private ICache<object, object> _metaCache;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            // Start 2 nodes.
            var cfg = TestUtils.GetTestConfiguration();
            var ignite = Ignition.Start(cfg);

            Ignition.Start(new IgniteConfiguration(cfg) {GridName = "grid2"});

            // Create SQL CE database in a temp file.
            using (var ctx = GetDbContext())
            {
                File.Delete(TempFile);
                ctx.Database.Create();
            }

            // Get the caches.
            _cache = ignite.GetCache<object, object>("entityFrameworkQueryCache_data")
                .WithKeepBinary<object, object>();

            _metaCache = ignite.GetCache<object, object>("entityFrameworkQueryCache_metadata")
                .WithKeepBinary<object, object>();
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            using (var ctx = GetDbContext())
            {
                ctx.Database.Delete();
            }

            Ignition.StopAll(true);
            File.Delete(TempFile);
        }

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void TestSetUp()
        {
            // Reset the policy.
            Policy.CanBeCachedFunc = null;
            Policy.CanBeCachedRowsFunc = null;
            Policy.GetExpirationTimeoutFunc = null;
            Policy.GetCachingStrategyFunc = null;

            // Clean up the db.
            using (var ctx = GetDbContext())
            {
                ctx.Blogs.RemoveRange(ctx.Blogs);
                ctx.Posts.RemoveRange(ctx.Posts);
                ctx.Tests.RemoveRange(ctx.Tests);

                ctx.SaveChanges();
            }

            using (var ctx = GetDbContext())
            {
                Assert.IsEmpty(ctx.Blogs);
                Assert.IsEmpty(ctx.Posts);
            }

            // Clear the caches.
            _cache.Clear();
            _metaCache.Clear();
        }

        /// <summary>
        /// Tests that caching actually happens.
        /// </summary>
        [Test]
        public void TestResultFromCache()
        {
            using (var ctx = GetDbContext())
            {
                // Add data.
                ctx.Posts.Add(new Post {Title = "Foo", Blog = new Blog(), PostId = 1});
                ctx.Posts.Add(new Post {Title = "Bar", Blog = new Blog(), PostId = 2});
                ctx.SaveChanges();

                Assert.AreEqual(new[] {"Foo"}, ctx.Posts.Where(x => x.Title == "Foo").Select(x => x.Title).ToArray());
                Assert.AreEqual(new[] {"Bar"}, ctx.Posts.Where(x => x.Title == "Bar").Select(x => x.Title).ToArray());

                // Alter cached data: swap cached values.
                
                var cachedData = _cache.ToArray();

                Assert.AreEqual(2, cachedData.Length);

                _cache[cachedData[0].Key] = cachedData[1].Value;
                _cache[cachedData[1].Key] = cachedData[0].Value;

                // Verify.
                Assert.AreEqual(new[] {"Bar"}, ctx.Posts.Where(x => x.Title == "Foo").Select(x => x.Title).ToArray());
                Assert.AreEqual(new[] {"Foo"}, ctx.Posts.Where(x => x.Title == "Bar").Select(x => x.Title).ToArray());
            }
        }

        /// <summary>
        /// Tests the read-write strategy (default).
        /// </summary>
        [Test]
        public void TestReadWriteStrategy()
        {
            using (var ctx = GetDbContext())
            {
                var blog = new Blog
                {
                    Name = "Foo",
                    Posts = new List<Post>
                    {
                        new Post {Title = "My First Post", Content = "Hello World!"}
                    }
                };
                ctx.Blogs.Add(blog);

                Assert.AreEqual(2, ctx.SaveChanges());

                // Check that query works.
                Assert.AreEqual(1, ctx.Posts.Where(x => x.Title.StartsWith("My")).ToArray().Length);

                // Add new post to check invalidation.
                ctx.Posts.Add(new Post {BlogId = blog.BlogId, Title = "My Second Post", Content = "Foo bar."});
                Assert.AreEqual(1, ctx.SaveChanges());

                Assert.AreEqual(0, _cache.GetSize()); // No cached entries.

                Assert.AreEqual(2, ctx.Posts.Where(x => x.Title.StartsWith("My")).ToArray().Length);

                Assert.AreEqual(1, _cache.GetSize()); // Cached query added.

                // Delete post.
                ctx.Posts.Remove(ctx.Posts.First());
                Assert.AreEqual(1, ctx.SaveChanges());

                Assert.AreEqual(0, _cache.GetSize()); // No cached entries.
                Assert.AreEqual(1, ctx.Posts.Where(x => x.Title.StartsWith("My")).ToArray().Length);

                Assert.AreEqual(1, _cache.GetSize()); // Cached query added.

                // Modify post.
                Assert.AreEqual(0, ctx.Posts.Count(x => x.Title.EndsWith("updated")));

                ctx.Posts.Single().Title += " - updated";
                Assert.AreEqual(1, ctx.SaveChanges());

                Assert.AreEqual(0, _cache.GetSize()); // No cached entries.
                Assert.AreEqual(1, ctx.Posts.Count(x => x.Title.EndsWith("updated")));

                Assert.AreEqual(1, _cache.GetSize()); // Cached query added.
            }
        }

        /// <summary>
        /// Tests the read only strategy.
        /// </summary>
        [Test]
        public void TestReadOnlyStrategy()
        {
            // Set up a policy to cache Blogs as read-only and Posts as read-write.
            Policy.GetCachingStrategyFunc = q =>
                q.AffectedEntitySets.Count == 1 && q.AffectedEntitySets.Single().Name == "Blog"
                    ? DbCachingMode.ReadOnly
                    : DbCachingMode.ReadWrite;

            using (var ctx = GetDbContext())
            {
                ctx.Blogs.Add(new Blog
                {
                    Name = "Foo",
                    Posts = new List<Post>
                    {
                        new Post {Title = "Post"}
                    }
                });

                ctx.SaveChanges();

                // Update entities.
                Assert.AreEqual("Foo", ctx.Blogs.Single().Name);
                Assert.AreEqual("Post", ctx.Posts.Single().Title);

                ctx.Blogs.Single().Name += " - updated";
                ctx.Posts.Single().Title += " - updated";

                ctx.SaveChanges();
            }

            // Verify that cached result is not changed for blogs, but changed for posts.
            using (var ctx = GetDbContext())
            {
                // Raw SQL queries do not hit cache - verify that actual data is updated.
                Assert.AreEqual("Foo - updated", ctx.Database.SqlQuery<string>("select name from blogs").Single());
                Assert.AreEqual("Post - updated", ctx.Database.SqlQuery<string>("select title from posts").Single());

                // Check EF queries that hit cache.
                Assert.AreEqual("Foo", ctx.Blogs.Single().Name);
                Assert.AreEqual("Post - updated", ctx.Posts.Single().Title);

            }

            // Clear the cache and verify that actual value in DB is changed.
            _cache.Clear();

            using (var ctx = GetDbContext())
            {
                Assert.AreEqual("Foo - updated", ctx.Blogs.Single().Name);
                Assert.AreEqual("Post - updated", ctx.Posts.Single().Title);
            }
        }

        /// <summary>
        /// Tests the scalar queries.
        /// </summary>
        [Test]
        public void TestScalars()
        {
            using (var ctx = GetDbContext())
            {
                var blog = new Blog
                {
                    Name = "Foo",
                    Posts = new List<Post>
                    {
                        new Post {Title = "1"},
                        new Post {Title = "2"},
                        new Post {Title = "3"},
                        new Post {Title = "4"}
                    }
                };
                ctx.Blogs.Add(blog);

                Assert.AreEqual(5, ctx.SaveChanges());

                // Test sum and count.
                const string esql = "SELECT COUNT(1) FROM [BloggingContext].Posts";

                Assert.AreEqual(4, ctx.Posts.Count());
                Assert.AreEqual(4, ctx.Posts.Count(x => x.Content == null));
                Assert.AreEqual(4, GetEntityCommand(ctx, esql).ExecuteScalar());
                Assert.AreEqual(blog.BlogId*4, ctx.Posts.Sum(x => x.BlogId));

                ctx.Posts.Remove(ctx.Posts.First());
                ctx.SaveChanges();

                Assert.AreEqual(3, ctx.Posts.Count());
                Assert.AreEqual(3, ctx.Posts.Count(x => x.Content == null));
                Assert.AreEqual(3, GetEntityCommand(ctx, esql).ExecuteScalar());
                Assert.AreEqual(blog.BlogId*3, ctx.Posts.Sum(x => x.BlogId));
            }
        }

        /// <summary>
        /// Queries with entity sets used multiple times are handled correctly.
        /// </summary>
        [Test]
        public void TestDuplicateEntitySets()
        {
            using (var ctx = GetDbContext())
            {
                var blog = new Blog
                {
                    Name = "Foo",
                    Posts = new List<Post>
                    {
                        new Post {Title = "Foo"},
                        new Post {Title = "Foo"},
                        new Post {Title = "Foo"},
                        new Post {Title = "Bar"}
                    }
                };
                ctx.Blogs.Add(blog);

                Assert.AreEqual(5, ctx.SaveChanges());

                var res = ctx.Blogs.Select(b => new
                {
                    X = b.Posts.FirstOrDefault(p => p.Title == b.Name),
                    Y = b.Posts.Count(p => p.Title == b.Name)
                }).ToArray();

                Assert.AreEqual(1, res.Length);
                Assert.AreEqual("Foo", res[0].X.Title);
                Assert.AreEqual(3, res[0].Y);

                // Modify and check updated result.
                ctx.Posts.Remove(ctx.Posts.First(x => x.Title == "Foo"));
                Assert.AreEqual(1, ctx.SaveChanges());

                res = ctx.Blogs.Select(b => new
                {
                    X = b.Posts.FirstOrDefault(p => p.Title == b.Name),
                    Y = b.Posts.Count(p => p.Title == b.Name)
                }).ToArray();

                Assert.AreEqual(1, res.Length);
                Assert.AreEqual("Foo", res[0].X.Title);
                Assert.AreEqual(2, res[0].Y);
            }
        }

        /// <summary>
        /// Tests transactions created with BeginTransaction.
        /// </summary>
        [Test]
        public void TestTx()
        {
            // Check TX without commit.
            using (var ctx = GetDbContext())
            {
                using (ctx.Database.BeginTransaction())
                {
                    ctx.Posts.Add(new Post {Title = "Foo", Blog = new Blog()});
                    ctx.SaveChanges();

                    Assert.AreEqual(1, ctx.Posts.ToArray().Length);
                }
            }

            using (var ctx = GetDbContext())
            {
                Assert.AreEqual(0, ctx.Posts.ToArray().Length);
            }

            // Check TX with commit.
            using (var ctx = GetDbContext())
            {
                using (var tx = ctx.Database.BeginTransaction())
                {
                    ctx.Posts.Add(new Post {Title = "Foo", Blog = new Blog()});
                    ctx.SaveChanges();

                    Assert.AreEqual(1, ctx.Posts.ToArray().Length);

                    tx.Commit();

                    Assert.AreEqual(1, ctx.Posts.ToArray().Length);
                }
            }

            using (var ctx = GetDbContext())
            {
                Assert.AreEqual(1, ctx.Posts.ToArray().Length);
            }
        }

        /// <summary>
        /// Tests transactions created with TransactionScope.
        /// </summary>
        [Test]
        public void TestTxScope()
        {
            // Check TX without commit.
            using (new TransactionScope())
            {
                using (var ctx = GetDbContext())
                {
                    ctx.Posts.Add(new Post {Title = "Foo", Blog = new Blog()});
                    ctx.SaveChanges();
                }
            }

            using (var ctx = GetDbContext())
            {
                Assert.AreEqual(0, ctx.Posts.ToArray().Length);
            }

            // Check TX with commit.
            using (var tx = new TransactionScope())
            {
                using (var ctx = GetDbContext())
                {
                    ctx.Posts.Add(new Post {Title = "Foo", Blog = new Blog()});
                    ctx.SaveChanges();
                }

                tx.Complete();
            }

            using (var ctx = GetDbContext())
            {
                Assert.AreEqual(1, ctx.Posts.ToArray().Length);
            }
        }

        /// <summary>
        /// Tests the expiration.
        /// </summary>
        [Test]
        public void TestExpiration()
        {
            Policy.GetExpirationTimeoutFunc = qry => TimeSpan.FromSeconds(0.3);

            using (var ctx = GetDbContext())
            {
                ctx.Posts.Add(new Post {Title = "Foo", Blog = new Blog()});
                ctx.SaveChanges();

                Assert.AreEqual(1, ctx.Posts.ToArray().Length);
                Assert.AreEqual(1, _cache.GetSize());

                var key = _cache.Single().Key;
                Assert.IsTrue(_cache.ContainsKey(key));

                Thread.Sleep(300);

                Assert.IsFalse(_cache.ContainsKey(key));
                Assert.AreEqual(0, _cache.GetSize());
                Assert.AreEqual(2, _metaCache.GetSize());
            }
        }

        /// <summary>
        /// Tests the caching policy.
        /// </summary>
        [Test]
        public void TestCachingPolicy()
        {
            var funcs = new List<string>();

            var checkQry = (Action<DbQueryInfo>) (qry =>
                {
                    var set = qry.AffectedEntitySets.Single();

                    Assert.AreEqual("Post", set.Name);

                    Assert.AreEqual(1, qry.Parameters.Count);
                    Assert.AreEqual(-5, qry.Parameters[0].Value);
                    Assert.AreEqual(DbType.Int32, qry.Parameters[0].DbType);

                    Assert.IsTrue(qry.CommandText.EndsWith("WHERE [Extent1].[BlogId] > @p__linq__0"));
                }
            );

            Policy.CanBeCachedFunc = qry =>
            {
                funcs.Add("CanBeCached");
                checkQry(qry);
                return true;
            };

            Policy.CanBeCachedRowsFunc = (qry, rows) =>
            {
                funcs.Add("CanBeCachedRows");
                Assert.AreEqual(3, rows);
                checkQry(qry);
                return true;
            };

            Policy.GetCachingStrategyFunc = qry =>
            {
                funcs.Add("GetCachingStrategy");
                checkQry(qry);
                return DbCachingMode.ReadWrite;
            };

            Policy.GetExpirationTimeoutFunc = qry =>
            {
                funcs.Add("GetExpirationTimeout");
                checkQry(qry);
                return TimeSpan.MaxValue;
            };

            using (var ctx = GetDbContext())
            {
                var blog = new Blog();

                ctx.Posts.Add(new Post {Title = "Foo", Blog = blog});
                ctx.Posts.Add(new Post {Title = "Bar", Blog = blog});
                ctx.Posts.Add(new Post {Title = "Baz", Blog = blog});

                ctx.SaveChanges();

                int minId = -5;
                Assert.AreEqual(3, ctx.Posts.Where(x => x.BlogId > minId).ToArray().Length);

                // Check that policy methods are called in correct order with correct params.
                Assert.AreEqual(
                    new[] {"GetCachingStrategy", "CanBeCached", "CanBeCachedRows", "GetExpirationTimeout"},
                    funcs.ToArray());
            }
        }

        /// <summary>
        /// Tests the cache reader indirectly with an entity that has various field types.
        /// </summary>
        [Test]
        public void TestCacheReader()
        {
            // Tests all kinds of entity field types to cover ArrayDbDataReader.
            var test = GetTestEntity();

            using (var ctx = new BloggingContext(ConnectionString))
            {
                ctx.Tests.Add(test);
                ctx.SaveChanges();
            }

            // Use new context to ensure no first-level caching.
            using (var ctx = new BloggingContext(ConnectionString))
            {
                // Check default deserialization.
                var test0 = ctx.Tests.Single(x => x.Bool);
                Assert.AreEqual(test, test0);
            }
        }

        /// <summary>
        /// Tests the cache reader by calling it directly.
        /// These calls are (partly) delegated by EF to the <see cref="ArrayDbDataReader"/>.
        /// </summary>
        [Test]
        public void TestCacheReaderRaw()
        {
            var test = GetTestEntity();

            using (var ctx = new BloggingContext(ConnectionString))
            {
                ctx.Tests.Add(test);
                ctx.SaveChanges();

                test = ctx.Tests.Single();
            }

            using (var ctx = new BloggingContext(ConnectionString))
            {
                var cmd = GetEntityCommand(ctx, "SELECT VALUE Test FROM BloggingContext.Tests AS Test");

                using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
                {
                    // Check schema.
                    Assert.Throws<NotSupportedException>(() => reader.GetSchemaTable());
                    Assert.AreEqual(0, reader.Depth);
                    Assert.AreEqual(-1, reader.RecordsAffected);
                    Assert.IsTrue(reader.HasRows);
                    Assert.IsFalse(reader.IsClosed);
                    Assert.AreEqual(11, reader.FieldCount);
                    Assert.AreEqual(11, reader.VisibleFieldCount);

                    // Check field names.
                    Assert.AreEqual("Edm.Int32", reader.GetDataTypeName(0));
                    Assert.AreEqual("Edm.Byte", reader.GetDataTypeName(1));
                    Assert.AreEqual("Edm.Int16", reader.GetDataTypeName(2));
                    Assert.AreEqual("Edm.Int64", reader.GetDataTypeName(3));
                    Assert.AreEqual("Edm.Single", reader.GetDataTypeName(4));
                    Assert.AreEqual("Edm.Double", reader.GetDataTypeName(5));
                    Assert.AreEqual("Edm.Decimal", reader.GetDataTypeName(6));
                    Assert.AreEqual("Edm.Boolean", reader.GetDataTypeName(7));
                    Assert.AreEqual("Edm.String", reader.GetDataTypeName(8));
                    Assert.AreEqual("Edm.Guid", reader.GetDataTypeName(9));
                    Assert.AreEqual("Edm.DateTime", reader.GetDataTypeName(10));

                    // Check field types.
                    Assert.AreEqual(typeof(int), reader.GetFieldType(0));
                    Assert.AreEqual(typeof(byte), reader.GetFieldType(1));
                    Assert.AreEqual(typeof(short), reader.GetFieldType(2));
                    Assert.AreEqual(typeof(long), reader.GetFieldType(3));
                    Assert.AreEqual(typeof(float), reader.GetFieldType(4));
                    Assert.AreEqual(typeof(double), reader.GetFieldType(5));
                    Assert.AreEqual(typeof(decimal), reader.GetFieldType(6));
                    Assert.AreEqual(typeof(bool), reader.GetFieldType(7));
                    Assert.AreEqual(typeof(string), reader.GetFieldType(8));
                    Assert.AreEqual(typeof(Guid), reader.GetFieldType(9));
                    Assert.AreEqual(typeof(DateTime), reader.GetFieldType(10));

                    // Read.
                    Assert.IsTrue(reader.Read());

                    // Test values array.
                    var vals = new object[reader.FieldCount];
                    reader.GetValues(vals);

                    Assert.AreEqual(test.Byte, vals[reader.GetOrdinal("Byte")]);
                    Assert.AreEqual(test.Short, vals[reader.GetOrdinal("Short")]);
                    Assert.AreEqual(test.ArrayReaderTestId, vals[reader.GetOrdinal("ArrayReaderTestId")]);
                    Assert.AreEqual(test.Long, vals[reader.GetOrdinal("Long")]);
                    Assert.AreEqual(test.Float, vals[reader.GetOrdinal("Float")]);
                    Assert.AreEqual(test.Double, vals[reader.GetOrdinal("Double")]);
                    Assert.AreEqual(test.Decimal, vals[reader.GetOrdinal("Decimal")]);
                    Assert.AreEqual(test.Bool, vals[reader.GetOrdinal("Bool")]);
                    Assert.AreEqual(test.String, vals[reader.GetOrdinal("String")]);
                    Assert.AreEqual(test.Guid, vals[reader.GetOrdinal("Guid")]);
                    Assert.AreEqual(test.DateTime, vals[reader.GetOrdinal("DateTime")]);
                }

                using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
                {
                    // Read.
                    Assert.IsTrue(reader.Read());

                    // Test separate values.
                    Assert.AreEqual(test.ArrayReaderTestId, reader.GetInt32(0));
                    Assert.AreEqual(test.Byte, reader.GetByte(1));
                    Assert.AreEqual(test.Short, reader.GetInt16(2));
                    Assert.AreEqual(test.Long, reader.GetInt64(3));
                    Assert.AreEqual(test.Float, reader.GetFloat(4));
                    Assert.AreEqual(test.Double, reader.GetDouble(5));
                    Assert.AreEqual(test.Decimal, reader.GetDecimal(6));
                    Assert.AreEqual(test.Bool, reader.GetBoolean(7));
                    Assert.AreEqual(test.String, reader.GetString(8));
                    Assert.AreEqual(test.Guid, reader.GetGuid(9));
                    Assert.AreEqual(test.DateTime, reader.GetDateTime(10));
                }
            }
        }

        /// <summary>
        /// Tests the database context.
        /// </summary>
        [Test]
        public void TestDbContext()
        {
            using (var ctx = GetDbContext())
            {
                var objCtx = ((IObjectContextAdapter) ctx).ObjectContext;

                var script = objCtx.CreateDatabaseScript();
                Assert.IsTrue(script.StartsWith("CREATE TABLE \"Blogs\""));
            }
        }

        /// <summary>
        /// Tests that old versions of caches entries are cleaned up.
        /// </summary>
        [Test]
        public void TestOldEntriesCleanup()
        {
            // Run in a loop to generate a bunch of outdated cache entries.
            for (var i = 0; i < 100; i++)
                CreateRemoveBlog();

            // Only one version of data is in the cache.
            Assert.AreEqual(1, _cache.GetSize());
            Assert.AreEqual(1, _metaCache.GetSize());
        }

        /// <summary>
        /// Tests the old entries cleanup in multi threaded scenario.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestOldEntriesCleanupMultithreaded()
        {
            TestUtils.RunMultiThreaded(CreateRemoveBlog, 4, 5);

            // Run once again to force cleanup.
            CreateRemoveBlog();

            // Wait for the cleanup to complete.
            Thread.Sleep(1000);

            // Only one version of data is in the cache.
            Assert.AreEqual(1, _cache.GetSize());
            Assert.AreEqual(1, _metaCache.GetSize());
        }

        /// <summary>
        /// Tests the entity set version increment in multi-threaded scenario.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestIncrementMultithreaded()
        {
            var opCnt = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                var blog = new Blog {Name = "my blog"};
                using (var ctx = GetDbContext())
                {
                    ctx.Blogs.Add(blog);
                    ctx.SaveChanges();
                }

                Interlocked.Increment(ref opCnt);

                using (var ctx = GetDbContext())
                {
                    ctx.Blogs.Attach(blog);
                    ctx.Blogs.Remove(blog);
                    ctx.SaveChanges();
                }

                Interlocked.Increment(ref opCnt);
            }, 4, 5);

            var setVersion = _metaCache["Blog"];

            Assert.AreEqual(opCnt, setVersion);
        }

        /// <summary>
        /// Creates and removes a blog.
        /// </summary>
        private void CreateRemoveBlog()
        {
            try
            {
                CreateRemoveBlog0();
            }
            catch (Exception ex)
            {
                // Ignore SQL CE glitch.
                if (!ex.ToString().Contains("The current row was deleted."))
                    throw;
            }
        }

        /// <summary>
        /// Creates and removes a blog.
        /// </summary>
        private void CreateRemoveBlog0()
        {
            var blog = new Blog {Name = "my blog"};
            var threadId = Thread.CurrentThread.ManagedThreadId;

            Func<object> getMeta = () => _metaCache.Where(x => x.Key.Equals("Blog"))
                .Select(x => x.Value).SingleOrDefault() ?? "null";

            var meta1 = getMeta();

            using (var ctx = GetDbContext())
            {
                ctx.Blogs.Add(blog);
                ctx.SaveChanges();
            }

            var meta2 = getMeta();

            using (var ctx = GetDbContext())
            {
                // Use ToArray so that there is always the same DB query.
                Assert.AreEqual(1, ctx.Blogs.ToArray().Count(x => x.BlogId == blog.BlogId),
                    string.Format("Existing blog not found: {0} = {1}, {2} | {3}", blog.BlogId, meta1, meta2, 
                    threadId));
            }

            var meta3 = getMeta();

            using (var ctx = GetDbContext())
            {
                ctx.Blogs.Attach(blog);
                ctx.Blogs.Remove(blog);
                ctx.SaveChanges();
            }

            var meta4 = getMeta();

            using (var ctx = GetDbContext())
            {
                // Use ToArray so that there is always the same DB query.
                Assert.AreEqual(0, ctx.Blogs.ToArray().Count(x => x.BlogId == blog.BlogId),
                    string.Format("Found removed blog: {0} = {1}, {2}, {3}, {4} | {5}", blog.BlogId, meta1, 
                    meta2, meta3, meta4, threadId));
            }
        }

        /// <summary>
        /// Executes the entity SQL.
        /// </summary>
        private static EntityCommand GetEntityCommand(IObjectContextAdapter ctx, string esql)
        {
            var objCtx = ctx.ObjectContext;

            var conn = objCtx.Connection;
            conn.Open();

            var cmd = (EntityCommand) conn.CreateCommand();
            cmd.CommandText = esql;

            return cmd;
        }

        /// <summary>
        /// Gets the test entity.
        /// </summary>
        private static ArrayReaderTest GetTestEntity()
        {
            return new ArrayReaderTest
            {
                DateTime = DateTime.Today,
                Bool = true,
                Byte = 56,
                String = "z",
                Decimal = (decimal)5.6,
                Double = 7.8d,
                Float = -4.5f,
                Guid = Guid.NewGuid(),
                ArrayReaderTestId = -8,
                Long = 3,
                Short = 5
            };
        }

        /// <summary>
        /// Gets the database context.
        /// </summary>
        private static BloggingContext GetDbContext()
        {
            return new BloggingContext(ConnectionString);
        }

        private class MyDbConfiguration : IgniteDbConfiguration
        {
            public MyDbConfiguration() : base(Ignition.GetIgnite(), null, null, Policy)
            {
                var ex = Assert.Throws<ArgumentException>(() => InitializeIgniteCaching(
                    this, Ignition.GetIgnite(), null, null, Policy));

                Assert.IsTrue(ex.Message.StartsWith("'dbConfiguration' argument is invalid: " +
                                                    "IgniteDbConfiguration.InitializeIgniteCaching should not " +
                                                    "be called for IgniteDbConfiguration instance."), ex.Message);
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

        private class DelegateCachingPolicy : DbCachingPolicy
        {
            public Func<DbQueryInfo, bool> CanBeCachedFunc { get; set; }

            public Func<DbQueryInfo, int, bool> CanBeCachedRowsFunc { get; set; }

            public Func<DbQueryInfo, TimeSpan> GetExpirationTimeoutFunc { get; set; }

            public Func<DbQueryInfo, DbCachingMode> GetCachingStrategyFunc { get; set; }

            public override bool CanBeCached(DbQueryInfo queryInfo)
            {
                return CanBeCachedFunc == null || CanBeCachedFunc(queryInfo);
            }

            public override bool CanBeCached(DbQueryInfo queryInfo, int rowCount)
            {
                return CanBeCachedRowsFunc == null || CanBeCachedRowsFunc(queryInfo, rowCount);
            }

            public override TimeSpan GetExpirationTimeout(DbQueryInfo queryInfo)
            {
                return GetExpirationTimeoutFunc == null 
                    ? base.GetExpirationTimeout(queryInfo) 
                    : GetExpirationTimeoutFunc(queryInfo);
            }

            public override DbCachingMode GetCachingMode(DbQueryInfo queryInfo)
            {
                return GetCachingStrategyFunc == null 
                    ? base.GetCachingMode(queryInfo)
                    : GetCachingStrategyFunc(queryInfo);
            }
        }
    }
}
