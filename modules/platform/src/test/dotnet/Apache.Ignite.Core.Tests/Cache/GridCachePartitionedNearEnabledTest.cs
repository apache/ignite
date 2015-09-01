/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Tests.Cache
{
    using NUnit.Framework;

    [Category(GridTestUtils.CATEGORY_INTENSIVE)]
    public class GridCachePartitionedNearEnabledTest : GridCacheAbstractTest
    {
        protected override int GridCount()
        {
            return 3;
        }

        protected override string CacheName()
        {
            return "partitioned_near";
        }

        protected override bool NearEnabled()
        {
            return true;
        }

        protected override bool TxEnabled()
        {
            return true;
        }

        protected override int Backups()
        {
            return 1;
        }
    }
}
