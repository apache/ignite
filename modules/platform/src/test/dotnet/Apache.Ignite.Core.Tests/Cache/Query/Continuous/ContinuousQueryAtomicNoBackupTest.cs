﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Tests.Cache.Query.Continuous
{
    /// <summary>
    /// Continuous query tests for ATOMIC cache with no backups.
    /// </summary>
    public class ContinuousQueryAtomiclNoBackupTest : ContinuousQueryNoBackupAbstractTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public ContinuousQueryAtomiclNoBackupTest()
            : base(CACHE_ATOMIC_NO_BACKUP)
        {
            // No-op.
        }
    }
}
