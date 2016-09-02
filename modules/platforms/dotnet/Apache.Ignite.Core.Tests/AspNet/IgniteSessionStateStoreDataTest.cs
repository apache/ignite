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

namespace Apache.Ignite.Core.Tests.AspNet
{
    using System.Reflection;
    using System.Web;
    using Apache.Ignite.AspNet.Impl;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IgniteSessionStateStoreData"/>.
    /// </summary>
    public class IgniteSessionStateStoreDataTest
    {
        /// <summary>
        /// Tests the data.
        /// </summary>
        [Test]
        public void TestData()
        {
            // Modification method is internal.
            var statics = new HttpStaticObjectsCollection();
            statics.GetType().GetMethod("Add", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(statics, new object[] { "int", typeof(int), false });

            var data = new IgniteSessionStateStoreData(statics, 44);

            data.Items["key"] = "val";

            Assert.AreEqual(44, data.Timeout);
            Assert.AreEqual(1, data.StaticObjects.Count);
            Assert.AreEqual(0, data.StaticObjects["int"]);
            Assert.AreEqual("val", data.Items["key"]);

            // Clone.
            var data1 = new IgniteSessionStateStoreData(data.Data);
            Assert.AreEqual(44, data1.Timeout);
            Assert.AreEqual(1, data1.StaticObjects.Count);
            Assert.AreEqual(0, data1.StaticObjects["int"]);
            Assert.AreEqual("val", data1.Items["key"]);
        }
    }
}
