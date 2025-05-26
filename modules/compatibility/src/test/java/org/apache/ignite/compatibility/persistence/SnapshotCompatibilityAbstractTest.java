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

package org.apache.ignite.compatibility.persistence;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;

/** */
public abstract class SnapshotCompatibilityAbstractTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    protected static final String OLD_IGNITE_VERSION = Collections.max(
        Arrays.asList(IgniteReleasedVersion.values()),
        Comparator.comparing(IgniteReleasedVersion::version)
    ).toString();

    /** */
    protected static final String SNAPSHOT_NAME = "test_snapshot";

    /** */
    protected static final String CACHE_DUMP_NAME = "test_cache_dump";

    /** */
    protected static final int BASE_CACHE_SIZE = 100;

    /** */
    protected static final int ENTRIES_CNT_FOR_INCREMENT = 100;

    /** */
    protected final CacheGroupsConfig cacheGrpsCfg = new CacheGroupsConfig(
        Set.of(
            new CacheGroupInfo("singleCache", Collections.singleton("singleCache")),
            new CacheGroupInfo("testCacheGrp", Set.of("testCache1", "testCache2"))
        )
    );

    /** */
    public static String consId(boolean customConsId, int nodeIdx) {
        return customConsId ? "node-" + nodeIdx : null;
    }

    /** */
    public static class SnapshotPathResolver {
        /** */
        private final boolean customSnpDir;

        /** */
        private final String workDirPath;

        /** */
        public SnapshotPathResolver(boolean customSnpDir) {
            this.customSnpDir = customSnpDir;

            try {
                workDirPath = U.defaultWorkDirectory();
            }
            catch (IgniteCheckedException ex) {
                throw new RuntimeException(ex);
            }
        }

        /** */
        public SnapshotPathResolver(boolean customSnpDir, String workDirPath) {
            this.customSnpDir = customSnpDir;
            this.workDirPath = workDirPath;
        }

        /** */
        public String snpDir(boolean delIfExist) throws IgniteCheckedException {
            return U.resolveWorkDirectory(workDirPath, customSnpDir ? "ex_snapshots" : "snapshots", delIfExist).getAbsolutePath();
        }

        /** */
        public String snpPath(String snpName, boolean delIfExist) throws IgniteCheckedException {
            return Paths.get(snpDir(delIfExist), snpName).toString();
        }
    }

    /** Configuration closure both for old and current Ignite version. */
    protected static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** */
        private final boolean incSnp;

        /** */
        private final String consId;

        /** */
        private final String snpDir;

        /** */
        private final boolean delIfExist;

        /** */
        private final CacheGroupsConfig cacheGrpsCfg;

        /** */
        private final String workDir;

        /** */
        public ConfigurationClosure(
            boolean incSnp,
            String consId,
            String snpDir,
            boolean delIfExist,
            CacheGroupsConfig cacheGrpsCfg,
            String workDir
        ) throws IgniteCheckedException {
            this.incSnp = incSnp;
            this.consId = consId;
            this.snpDir = snpDir;
            this.delIfExist = delIfExist;
            this.cacheGrpsCfg = cacheGrpsCfg;
            this.workDir = workDir;
        }

        /** */
        public ConfigurationClosure(
            boolean incSnp,
            String consId,
            String snpDir,
            boolean delIfExist,
            CacheGroupsConfig cacheGrpsCfg
        ) throws IgniteCheckedException {
            this.incSnp = incSnp;
            this.consId = consId;
            this.snpDir = snpDir;
            this.delIfExist = delIfExist;
            this.cacheGrpsCfg = cacheGrpsCfg;
            workDir = U.defaultWorkDirectory();
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setWorkDirectory(workDir);

            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

            cfg.setDataStorageConfiguration(storageCfg);

            cfg.setConsistentId(consId);

            storageCfg.setWalCompactionEnabled(incSnp);

            if (delIfExist) {
                cfg.setCacheConfiguration(
                    cacheGrpsCfg.cacheGroupInfos().stream()
                        .flatMap(cacheGrpInfo ->
                            cacheGrpInfo.cacheNames().stream()
                            .map(cacheName -> new CacheConfiguration<Integer, String>(cacheName)
                                .setGroupName(cacheGrpInfo.name())
                                .setAffinity(new RendezvousAffinityFunction(false, 10))
                            )
                        )
                        .toArray(CacheConfiguration[]::new)
                );
            }

            cfg.setSnapshotPath(snpDir);
        }
    }

    /** Snapshot creating closure both for old and current Ignite version. */
    protected static class CreateSnapshotClosure implements IgniteInClosure<Ignite> {
        /** */
        private final boolean incSnp;

        /** */
        private final boolean cacheDump;

        /** */
        private final CacheGroupsConfig cacheGrpsCfg;

        /** */
        public CreateSnapshotClosure(boolean incSnp, boolean cacheDump, CacheGroupsConfig cacheGrpsCfg) {
            this.incSnp = incSnp;
            this.cacheDump = cacheDump;
            this.cacheGrpsCfg = cacheGrpsCfg;
        }

        /** {@inheritDoc} */
        @Override public void apply(Ignite ign) {
            ign.cluster().state(ClusterState.ACTIVE);

            cacheGrpsCfg.cacheGroupInfos().forEach(cacheGrpInfo -> cacheGrpInfo.addItemsToCacheGrp(ign, 0, BASE_CACHE_SIZE));

            if (cacheDump)
                ign.snapshot().createDump(CACHE_DUMP_NAME, cacheGrpsCfg.cacheGroupNames()).get();
            else
                ign.snapshot().createSnapshot(SNAPSHOT_NAME).get();

            if (incSnp) {
                cacheGrpsCfg.cacheGroupInfos().forEach(
                    cacheGrpInfo -> cacheGrpInfo.addItemsToCacheGrp(ign, BASE_CACHE_SIZE, ENTRIES_CNT_FOR_INCREMENT)
                );

                ign.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get();
            }
        }
    }

    /** */
    protected static class CacheGroupsConfig {
        /** */
        private final Map<String, CacheGroupInfo> cacheGrpInfos = new HashMap<>();

        /** */
        public CacheGroupsConfig() {
            // No-op
        }

        /** */
        public CacheGroupsConfig(Set<CacheGroupInfo> cacheGrpInfos) {
            cacheGrpInfos.forEach(cacheGrpInfo -> this.cacheGrpInfos.put(cacheGrpInfo.name(), cacheGrpInfo));
        }

        /** */
        public Collection<CacheGroupInfo> cacheGroupInfos() {
            return cacheGrpInfos.values();
        }

        /** */
        public Set<String> cacheGroupNames() {
            return new HashSet<>(cacheGrpInfos.keySet());
        }

        /** */
        public Set<String> cacheNames() {
            return cacheGrpInfos.values().stream().flatMap(cacheGrpInfo -> cacheGrpInfo.cacheNames().stream()).collect(Collectors.toSet());
        }

        /** */
        public void addCache(String cacheGrpName, String cacheName) {
            cacheGrpInfos.computeIfAbsent(cacheGrpName, CacheGroupInfo::new).addCacheName(cacheName);
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof CacheGroupsConfig))
                return false;

            return Objects.equals(cacheGrpInfos, ((CacheGroupsConfig)o).cacheGrpInfos);
        }

        /** */
        @Override public int hashCode() {
            return Objects.hashCode(cacheGrpInfos);
        }
    }

    /** */
    protected static class CacheGroupInfo {
        /** */
        private final String name;

        /** */
        private final Set<String> cacheNames;

        /** */
        public CacheGroupInfo(String name) {
            this.name = name;

            cacheNames = new HashSet<>();
        }

        /** */
        public CacheGroupInfo(String name, Set<String> cacheNames) {
            this.name = name;
            this.cacheNames = cacheNames;
        }

        /** */
        public boolean addCacheName(String cacheName) {
            return cacheNames.add(cacheName);
        }

        /** */
        public String name() {
            return name;
        }

        /** */
        public Set<String> cacheNames() {
            return cacheNames;
        }

        /** */
        public static String calcValue(String cacheName, int key) {
            return cacheName + "-organization-" + key;
        }

        /** */
        public void addItemsToCacheGrp(Ignite ign, int startIdx, int cnt) {
            for (String cacheName : cacheNames)
                addItemsToCache(ign.cache(cacheName), startIdx, cnt);
        }

        /** */
        public void checkCaches(Ignite ign, int expectedCacheSize) {
            for (String cacheName : cacheNames) {
                IgniteCache<Integer, String> cache = ign.cache(cacheName);

                assertNotNull(cache);

                checkCache(cache, expectedCacheSize);
            }
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof CacheGroupInfo))
                return false;

            CacheGroupInfo that = (CacheGroupInfo)o;

            return Objects.equals(name, that.name) && Objects.equals(cacheNames, that.cacheNames);
        }

        /** */
        @Override public int hashCode() {
            return Objects.hashCode(name);
        }

        /** */
        private void checkCache(IgniteCache<Integer, String> cache, int expectedSize) {
            assertEquals(expectedSize, cache.size());

            for (int i = 0; i < expectedSize; ++i)
                assertEquals(calcValue(cache.getName(), i), cache.get(i));
        }

        /** */
        private void addItemsToCache(IgniteCache<Integer, String> cache, int startIdx, int cnt) {
            for (int i = startIdx; i < startIdx + cnt; ++i)
                cache.put(i, calcValue(cache.getName(), i));
        }
    }
}