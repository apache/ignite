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

package org.apache.ignite.internal.processors.cache.persistence.db;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccLeafIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class IgniteTcBotSandboxTest extends GridCommonAbstractTest {

    public static final String TEST_HIST_CACHE_NAME = "teamcityTestRunHist";

    // todo set correct path
    public static final String WORK_DIR = "C:\\projects\\corrupt\\work";

    @Test
    public void readTcBotDb() throws Exception {

        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS, H2MvccInnerIO.VERSIONS, H2MvccLeafIO.VERSIONS);
        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();

        int grpId = U.safeAbs(TEST_HIST_CACHE_NAME.hashCode());
        System.out.println("Cache name hash code: " + grpId);
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);
        WALIterator iter = factory.iterator(new File(WORK_DIR, "db\\wal\\archive\\TcHelper").getAbsolutePath());

        long pageId = 0x00010000000145b4L;
        processRecords(grpId, iter, pageId);

        WALIterator iter2 = factory.iterator(new File(WORK_DIR, "db\\wal\\TcHelper").getAbsolutePath());

        processRecords(grpId, iter2, pageId);

        IgniteEx ignite = startGrid(0);
        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite.cache(TEST_HIST_CACHE_NAME);
        Preconditions.checkNotNull(cache, "Cache should be present, present caches ["  + ignite.cacheNames() + "]");
        IgniteCache<BinaryObject, BinaryObject> entries = cache.withKeepBinary();
        IgniteBinary binary = ignite.binary();
        BinaryObjectBuilder builder = binary.builder("org.apache.ignite.ci.teamcity.ignited.runhist.RunHistKey");

        builder.setField("srvId", 1411517106);
        builder.setField("testOrSuiteName", 11924);
        builder.setField("branch", 281);

        //idHash=1028871081, hash=1241170874, srvId=1411517106, testOrSuiteName=11924, branch=281

        BinaryObject key = builder.build();
        BinaryObject val = entries.get(key);

        System.out.println(val);
    }

    private void processRecords(int grpId, WALIterator iter, long pageId) {
        Set<Long> segments = new HashSet<>();
        while (iter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> next = iter.next();
            WALPointer pos = next.get1();
            WALRecord rec = next.get2();
            FileWALPointer walPtr = (FileWALPointer)pos;
            long idx = walPtr.index();

            if (segments.add(idx)) {
                System.out.println("Scanning segment [" + idx + "]," +
                    " relIdx [" + (idx % 10) + "]");
            }

            if (rec instanceof InitNewPageRecord) {
                InitNewPageRecord initRec = (InitNewPageRecord)rec;

                if (initRec.groupId() == grpId
                    && (initRec.pageId() == pageId || initRec.newPageId() == pageId)) {
                    System.out.println("InitNewPageRecord:: pos=" + pos
                        + ", rec=" + initRec.toString() + ", ioType=" + initRec.ioType()
                        + ", ioVersion=" + initRec.ioVersion());

                    assertEquals(PageIdUtils.partId(initRec.newPageId()),
                        PageIdUtils.partId(initRec.pageId()));
                }
            }
            else if (rec instanceof PageSnapshot) {
                PageSnapshot snapshot = (PageSnapshot)rec;

                if (snapshot.fullPageId().groupId() == grpId
                    && snapshot.fullPageId().pageId() == pageId) {
                    System.out.println("PageSnapshot:: pos=" + pos
                        + ", rec=" + snapshot.toString());
                }
            } else if(rec instanceof PageDeltaRecord) {
                PageDeltaRecord deltaRec = (PageDeltaRecord)rec;

                if (deltaRec.groupId() == grpId
                    && deltaRec.pageId() == pageId) {
                    System.out.println("PageDeltaRecord:: pos=" + pos
                        + ", rec=" + deltaRec.toString());
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setWorkDirectory(WORK_DIR);

        cfg.setConsistentId("TcHelper");

        DataRegionConfiguration regCfg = new DataRegionConfiguration()
            .setMaxSize(2L * 1024 * 1024 * 1024)
            .setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(regCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }
}
