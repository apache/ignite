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
    using Apache.Ignite.Core.Common;
    using GridGain.Common;
    using NUnit.Framework;

    /// <summary>
    /// Test marshaller initialization.
    /// </summary>
    public class GridMarshallerTest
    {
        /// <summary>
        /// Tests the default marhsaller.
        /// By default, portable marshaller is used.
        /// </summary>
        [Test]
        public void TestDefaultMarhsaller()
        {
            using (var grid = GridFactory.Start("config\\marshaller-default.xml"))
            {
                var cache = grid.GetOrCreateCache<int, int>(null);

                cache.Put(1, 1);

                Assert.AreEqual(1, cache.Get(1));
            }
        }

        /// <summary>
        /// Tests the portable marhsaller.
        /// PortableMarshaller can be specified explicitly in config.
        /// </summary>
        [Test]
        public void TestPortableMarhsaller()
        {
            using (var grid = GridFactory.Start("config\\marshaller-portable.xml"))
            {
                var cache = grid.GetOrCreateCache<int, int>(null);

                cache.Put(1, 1);

                Assert.AreEqual(1, cache.Get(1));
            }
        }

        /// <summary>
        /// Tests the invalid marshaller.
        /// </summary>
        [Test]
        public void TestInvalidMarshaller()
        {
            Assert.Throws<IgniteException>(() => GridFactory.Start("config\\marshaller-invalid.xml"));
        }
    }
}