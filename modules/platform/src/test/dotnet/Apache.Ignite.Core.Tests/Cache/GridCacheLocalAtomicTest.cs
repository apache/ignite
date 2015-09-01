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
    public class GridCacheLocalAtomicTest : GridCacheAbstractTest
    {
        protected override int CachePartitions()
        {
            return 1;
        }

        protected override int GridCount()
        {
            return 1;
        }

        protected override string CacheName()
        {
            return "local_atomic";
        }

        protected override bool NearEnabled()
        {
            return false;
        }

        protected override bool TxEnabled()
        {
            return false;
        }

        protected override bool LocalCache()
        {
            return true;
        }

        protected override int Backups()
        {
            return 0;
        }
    }
}
