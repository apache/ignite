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

namespace Apache.Ignite.Core.Tests.Binary.Serializable
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Reflection;
    using System.Reflection.Emit;
    using System.Runtime.Serialization;
    using System.Xml;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests additional [Serializable] scenarios.
    /// </summary>
    public class AdvancedSerializationTest
    {
        /// <summary>
        /// Set up routine.
        /// </summary>
        [TestFixtureSetUp]
        public void SetUp()
        {
            Ignition.Start(TestUtils.GetTestConfiguration());
        }

        /// <summary>
        /// Tear down routine.
        /// </summary>
        [TestFixtureTearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Test complex file serialization.
        /// </summary>
        [Test]
        public void TestSerializableXmlDoc()
        {
            var grid = Ignition.GetIgnite(null);
            var cache = grid.GetOrCreateCache<int, SerializableXmlDoc>("cache");

            var doc = new SerializableXmlDoc();

            doc.LoadXml("<document><test1>val</test1><test2 attr=\"x\" /></document>");

            for (var i = 0; i < 50; i++)
            {
                // Test cache
                cache.Put(i, doc);

                var resultDoc = cache.Get(i);

                Assert.AreEqual(doc.OuterXml, resultDoc.OuterXml);

                // Test task with document arg
                CheckTask(grid, doc);
            }
        }

        /// <summary>
        /// Checks task execution.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="arg">Task arg.</param>
        private static void CheckTask(IIgnite grid, object arg)
        {
            var jobResult = grid.GetCompute().Execute(new CombineStringsTask(), arg);

            var nodeCount = grid.GetCluster().GetNodes().Count;

            var expectedRes =
                CombineStringsTask.CombineStrings(Enumerable.Range(0, nodeCount).Select(x => arg.ToString()));

            Assert.AreEqual(expectedRes, jobResult.InnerXml);
        }

#if !NETCOREAPP2_0 && !NETCOREAPP2_1 && !NETCOREAPP3_0  // AppDomains are not supported in .NET Core
        /// <summary>
        /// Tests custom serialization binder.
        /// </summary>
        [Test]
        public void TestSerializationBinder()
        {
            const int count = 50;

            var cache = Ignition.GetIgnite(null).GetOrCreateCache<int, object>("cache");

            // Put multiple objects from multiple same-named assemblies to cache
            for (var i = 0; i < count; i++)
            {
                dynamic val = Activator.CreateInstance(GenerateDynamicType());

                val.Id = i;
                val.Name = "Name_" + i;

                cache.Put(i, val);
            }

            // Verify correct deserialization
            for (var i = 0; i < count; i++)
            {
                dynamic val = cache.Get(i);

                Assert.AreEqual(val.Id, i);
                Assert.AreEqual(val.Name, "Name_" + i);
            }
        }

        /// <summary>
        /// Generates a Type in runtime, puts it into a dynamic assembly.
        /// </summary>
        /// <returns></returns>
        private static Type GenerateDynamicType()
        {
            var asmBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(
                new AssemblyName("GridSerializationTestDynamicAssembly"), AssemblyBuilderAccess.Run);

            var moduleBuilder = asmBuilder.DefineDynamicModule("GridSerializationTestDynamicModule");

            var typeBuilder = moduleBuilder.DefineType("GridSerializationTestDynamicType",
                TypeAttributes.Class | TypeAttributes.Public | TypeAttributes.Serializable);

            typeBuilder.DefineField("Id", typeof (int), FieldAttributes.Public);

            typeBuilder.DefineField("Name", typeof (string), FieldAttributes.Public);

            return typeBuilder.CreateType();
        }
#endif

        /// <summary>
        /// Tests the DataTable serialization.
        /// </summary>
        [Test]
        public void TestDataTable()
        {
            var dt = new DataTable("foo");

            dt.Columns.Add("intCol", typeof(int));
            dt.Columns.Add("stringCol", typeof(string));

            dt.Rows.Add(1, "1");
            dt.Rows.Add(2, "2");

            var cache = Ignition.GetIgnite().GetOrCreateCache<int, DataTable>("dataTables");
            cache.Put(1, dt);

            var res = cache.Get(1);

            Assert.AreEqual("foo", res.TableName);

            Assert.AreEqual(2, res.Columns.Count);
            Assert.AreEqual("intCol", res.Columns[0].ColumnName);
            Assert.AreEqual("stringCol", res.Columns[1].ColumnName);

            Assert.AreEqual(2, res.Rows.Count);
            Assert.AreEqual(new object[] {1, "1"}, res.Rows[0].ItemArray);
            Assert.AreEqual(new object[] {2, "2"}, res.Rows[1].ItemArray);
        }
    }

    [Serializable]
    [DataContract]
    public sealed class SerializableXmlDoc : XmlDocument, ISerializable
    {
        /// <summary>
        /// Default ctor.
        /// </summary>
        public SerializableXmlDoc()
        {
            // No-op
        }

        /// <summary>
        /// Serialization ctor.
        /// </summary>
        private SerializableXmlDoc(SerializationInfo info, StreamingContext context)
        {
            LoadXml(info.GetString("xmlDocument"));
        }

        /** <inheritdoc /> */
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("xmlDocument", OuterXml, typeof(string));
        }
    }

    [Serializable]
    public class CombineStringsTask : IComputeTask<object, string, SerializableXmlDoc>
    {
        public IDictionary<IComputeJob<string>, IClusterNode> Map(IList<IClusterNode> subgrid, object arg)
        {
            return subgrid.ToDictionary(x => (IComputeJob<string>) new ToStringJob {Arg = arg}, x => x);
        }

        public ComputeJobResultPolicy OnResult(IComputeJobResult<string> res, IList<IComputeJobResult<string>> rcvd)
        {
            return ComputeJobResultPolicy.Wait;
        }

        public SerializableXmlDoc Reduce(IList<IComputeJobResult<string>> results)
        {
            var result = new SerializableXmlDoc();

            result.LoadXml(CombineStrings(results.Select(x => x.Data)));

            return result;
        }

        public static string CombineStrings(IEnumerable<string> strings)
        {
            var text = string.Concat(strings.Select(x => string.Format("<val>{0}</val>", x)));

            return string.Format("<document>{0}</document>", text);
        }
    }

    [Serializable]
    public class ToStringJob : IComputeJob<string>
    {
        /// <summary>
        /// Job argument.
        /// </summary>
        public object Arg { get; set; }

        /** <inheritdoc /> */
        public string Execute()
        {
            return Arg.ToString();
        }

        /** <inheritdoc /> */
        public void Cancel()
        {
            // No-op.
        }
    }
}
