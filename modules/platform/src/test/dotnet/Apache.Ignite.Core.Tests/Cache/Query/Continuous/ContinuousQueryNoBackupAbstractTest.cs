﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Cache.Query.Continuous
{
    using NUnit.Framework;

    /// <summary>
    /// Tests for ocntinuous query when there are no backups.
    /// </summary>
    public abstract class ContinuousQueryNoBackupAbstractTest : ContinuousQueryAbstractTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cacheName">Cache name.</param>
        protected ContinuousQueryNoBackupAbstractTest(string cacheName) : base(cacheName)
        {
            // No-op.
        }

        /// <summary>
        /// Test regular callback operations for local query.
        /// </summary>
        [Test]
        public void TestCallbackLocal()
        {
            CheckCallback(true);
        }

        /// <summary>
        /// Test portable filter logic.
        /// </summary>
        [Test]
        public void TestFilterPortableLocal()
        {
            CheckFilter(true, true);
        }

        /// <summary>
        /// Test serializable filter logic.
        /// </summary>
        [Test]
        public void TestFilterSerializableLocal()
        {
            CheckFilter(false, true);
        }

        /// <summary>
        /// Test non-serializable filter for local query.
        /// </summary>
        [Test]
        public void TestFilterNonSerializableLocal()
        {
            CheckFilterNonSerializable(true);
        }
    }
}
