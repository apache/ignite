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

package org.apache.ignite.internal.processors.igfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutOfSpaceException;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ThreadLocalRandom8;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * {@link IgfsAttributes} test case.
 */
public class IgfsDeallocateFreeSpaceTest extends IgfsAbstractBaseSelfTest {
    /** Maximum amount of bytes that could be written to particular file. */
    private static final int IGFS_SIZE_LIMIT = 10_000_000;

    private static final int SMALL_FILE_SIZE = 4_000_000;
    private static final int BIG_FILE_SIZE = 9_000_000;

    /**
     *
     */
    public IgfsDeallocateFreeSpaceTest() {
        super(IgfsMode.PRIMARY);
    }

    /** {@inheritDoc} */
    @Override protected FileSystemConfiguration getFileSystemConfiguration(String igfsName, IgfsMode mode,
        @Nullable IgfsSecondaryFileSystem secondaryFs, @Nullable IgfsIpcEndpointConfiguration restCfg) {
        FileSystemConfiguration cfg = super.getFileSystemConfiguration(igfsName, mode, secondaryFs, restCfg);
        cfg.setMaxSpaceSize(IGFS_SIZE_LIMIT);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeallocate() throws Exception {
        byte [] small = new byte[SMALL_FILE_SIZE];
        byte [] big = new byte[BIG_FILE_SIZE];

        System.out.println("+++ Free space: " + freeSpace());
        createFile(igfs, new IgfsPath("/small"), true, small);
        dumpCache("MetaCache" , getMetaCache(igfs));
        dumpCache("DataCache" , getDataCache(igfs));



        System.out.println("+++ Free space: " + freeSpace());
        createFile(igfs, new IgfsPath("/big1"), true, big);
        System.out.println("+++ big1 len: " + igfs.info(new IgfsPath("/big1")).length());
        System.out.println("+++ Free space: " + freeSpace());
        dumpCache("MetaCache" , getMetaCache(igfs));
        dumpCache("DataCache" , getDataCache(igfs));



        igfs.format();
        log.info("+++ Free space: " + freeSpace());

        if(IGFS_SIZE_LIMIT > freeSpace()) {
            dumpCache("MetaCache" , getMetaCache(igfs));

            dumpCache("DataCache" , getDataCache(igfs));

            assert false;
        }
    }

    /**
     * @return IGFS free space.
     */
    long freeSpace() {
        IgfsStatus s = igfs.globalSpace();
        return s.spaceTotal() - s.spaceUsed();
    }


}