/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using GridGain.Cache;
    using GridGain.Portable;

    using NUnit.Framework;

    public class GridPortableConfigurationTest
    {
        /** Cache. */
        private ICache<int, TestGenericPortableBase> cache;

        /** Random generator. */
        private static readonly Random RND = new Random();

        /** Test types for code config */
        private static readonly Type[] TEST_TYPES = {
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
        private static readonly Type[] TEST_TYPES_XML = {
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
            GridFactory.StopAll(true);

            var grid = GridFactory.Start(new GridConfiguration
            {
                SpringConfigUrl = "config\\cache-portables.xml",
                JvmClasspath = GridTestUtils.CreateTestClasspath(),
                JvmOptions = GridTestUtils.TestJavaOptions(),
                PortableConfiguration = portableConfiguration
            });

            cache = grid.Cache<int, TestGenericPortableBase>(null);
        }

        /// <summary>
        /// Test fixture tear-down routine.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            GridTestUtils.KillProcesses();
        }

        /// <summary>
        /// Tests the configuration set in code.
        /// </summary>
        [Test]
        public void TestCodeConfiguration()
        {
            StartGrid(new PortableConfiguration
            {
                TypeConfigurations = TEST_TYPES.Select(x => new PortableTypeConfiguration(x)).ToList()
            });

            CheckPortableTypes(TEST_TYPES);
        }

        /// <summary>
        /// Tests the configuration set in xml.
        /// </summary>
        [Test]
        public void TestXmlConfiguration()
        {
            StartGrid(null);

            CheckPortableTypes(TEST_TYPES_XML);
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

                cache.Put(key, inst);

                var result = cache.Get(key);

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

            inst.Prop = RND.Next(int.MaxValue);

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