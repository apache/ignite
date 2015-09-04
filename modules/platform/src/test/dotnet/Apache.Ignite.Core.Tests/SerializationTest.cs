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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Reflection.Emit;
    using System.Runtime.Serialization;
    using System.Xml;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl;
    using NUnit.Framework;

    /// <summary>
    /// Tests for native serialization.
    /// </summary>
    public class SerializationTest
    {
        /** Grid name. */
        private const string GridName = "SerializationTest";

        /// <summary>
        /// Set up routine.
        /// </summary>
        [TestFixtureSetUp]
        public void SetUp()
        {
            var cfg = new IgniteConfigurationEx
            {
                GridName = GridName,
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                SpringConfigUrl = "config\\native-client-test-cache.xml"
            };

            Ignition.Start(cfg);
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
            var grid = Ignition.GetIgnite(GridName);
            var cache = grid.Cache<int, SerializableXmlDoc>("replicated");

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
            var jobResult = grid.Compute().Execute(new CombineStringsTask(), arg);

            var nodeCount = grid.Cluster.Nodes().Count;

            var expectedRes =
                CombineStringsTask.CombineStrings(Enumerable.Range(0, nodeCount).Select(x => arg.ToString()));

            Assert.AreEqual(expectedRes, jobResult.InnerXml);
        }

        /// <summary>
        /// Tests custom serialization binder.
        /// </summary>
        [Test]
        public void TestSerializationBinder()
        {
            const int count = 50;

            var cache = Ignition.GetIgnite(GridName).Cache<int, object>("local");

            // Put multiple objects from muliple same-named assemblies to cache
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
        public static Type GenerateDynamicType()
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

        public ComputeJobResultPolicy Result(IComputeJobResult<string> res, IList<IComputeJobResult<string>> rcvd)
        {
            return ComputeJobResultPolicy.Wait;
        }

        public SerializableXmlDoc Reduce(IList<IComputeJobResult<string>> results)
        {
            var result = new SerializableXmlDoc();

            result.LoadXml(CombineStrings(results.Select(x => x.Data())));

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