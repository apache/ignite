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
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Portable;
    using NUnit.Framework;

    /// <summary>
    /// Portable configuration tests.
    /// </summary>
    public class PortableConfigurationTest
    {
        /** Cache. */
        private ICache<int, TestGenericPortableBase> _cache;

        /** Random generator. */
        private static readonly Random Rnd = new Random();

        /** Test types for code config */
        private static readonly Type[] TestTypes = {
            typeof (TestGenericPortable<int>),
            typeof (TestGenericPortable<string>),
            typeof (TestGenericPortable<TestGenericPortable<int>>),
            typeof (TestGenericPortable<List<Tuple<int, string>>>),
            typeof (TestGenericPortable<int, string>),
            typeof (TestGenericPortable<int, TestGenericPortable<string>>),
            typeof (TestGenericPortable<int, string, Type>),
            typeof (TestGenericPortable<int, string, TestGenericPortable<int, string, Type>>)
        };

        /** Test types for xml config */
        private static readonly Type[] TestTypesXml = {
            typeof (TestGenericPortable<long>),
            typeof (TestGenericPortable<Type>),
            typeof (TestGenericPortable<TestGenericPortable<long>>),
            typeof (TestGenericPortable<List<Tuple<long, string>>>),
            typeof (TestGenericPortable<long, string>),
            typeof (TestGenericPortable<long, TestGenericPortable<string>>),
            typeof (TestGenericPortable<long, string, Type>),
            typeof (TestGenericPortable<long, string, TestGenericPortable<long, string, Type>>)
        };

        /// <summary>
        /// Starts the grid with provided config.
        /// </summary>
        /// <param name="portableConfiguration">The portable configuration.</param>
        private void StartGrid(PortableConfiguration portableConfiguration)
        {
            Ignition.StopAll(true);

            var grid = Ignition.Start(new IgniteConfiguration
            {
                SpringConfigUrl = "config\\cache-portables.xml",
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                PortableConfiguration = portableConfiguration
            });

            _cache = grid.Cache<int, TestGenericPortableBase>(null);
        }

        /// <summary>
        /// Test fixture tear-down routine.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            TestUtils.KillProcesses();
        }

        /// <summary>
        /// Tests the configuration set in code.
        /// </summary>
        [Test]
        public void TestCodeConfiguration()
        {
            StartGrid(new PortableConfiguration
            {
                TypeConfigurations = TestTypes.Select(x => new PortableTypeConfiguration(x)).ToList()
            });

            CheckPortableTypes(TestTypes);
        }

        /// <summary>
        /// Tests the configuration set in xml.
        /// </summary>
        [Test]
        public void TestXmlConfiguration()
        {
            StartGrid(null);

            CheckPortableTypes(TestTypesXml);
        }

        /// <summary>
        /// Checks that specified types are portable and can be successfully used in cache.
        /// </summary>
        private void CheckPortableTypes(IEnumerable<Type> testTypes)
        {
            int key = 0;

            foreach (var typ in testTypes)
            {
                key += 1;

                var inst = CreateInstance(typ);

                _cache.Put(key, inst);

                var result = _cache.Get(key);

                Assert.AreEqual(inst.Prop, result.Prop);

                Assert.AreEqual(typ, result.GetType());
            }
        }

        /// <summary>
        /// Creates the instance of specified test portable type and sets a value on it.
        /// </summary>
        private static TestGenericPortableBase CreateInstance(Type type)
        {
            var inst = (TestGenericPortableBase)Activator.CreateInstance(type);

            inst.Prop = Rnd.Next(int.MaxValue);

            return inst;
        }
    }

    public abstract class TestGenericPortableBase
    {
        public object Prop { get; set; }
    }

    public class TestGenericPortable<T> : TestGenericPortableBase
    {
        public T Prop1 { get; set; }
    }

    public class TestGenericPortable<T1, T2> : TestGenericPortableBase
    {
        public T1 Prop1 { get; set; }
        public T2 Prop2 { get; set; }
    }

    public class TestGenericPortable<T1, T2, T3> : TestGenericPortableBase
    {
        public T1 Prop1 { get; set; }
        public T2 Prop2 { get; set; }
        public T3 Prop3 { get; set; }
    }
}