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
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Data.Entity;
    using System.Data.Entity.Infrastructure;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.EntityFramework;
    using NUnit.Framework;

    /// <summary>
    /// Integration test with in-memory database.
    /// </summary>
    public class EntityFrameworkSecondLevelCacheTest
    {
        // https://www.stevefenton.co.uk/2015/11/using-an-in-memory-database-as-a-test-double-with-entity-framework/

        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Ignition.Start(TestUtils.GetTestConfiguration());
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        [Test]
        public void Test()
        {
            var context = CreateTempDatabase();
            context.Database.Log = s => Debug.WriteLine(s);

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

            Assert.AreEqual(1, context.Posts.Where(x => x.Title.StartsWith("My")).ToArray().Length);
        }

        private static BloggingContext CreateTempDatabase()
        {
            // TODO: Delete this file afterwards.
            var filePath = Path.GetTempFileName();
            File.Delete(filePath);
            var connectionString = "Datasource = " + filePath;

#pragma warning disable 618
            //Database.DefaultConnectionFactory = new SqlCeConnectionFactory("System.Data.SqlServerCe.4.0");
#pragma warning restore 618

            var context = new BloggingContext(connectionString);

            context.Database.Create();

            return context;
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
        }
    }
}
