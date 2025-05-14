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

package org.apache.ignite.internal.processors.performancestatistics;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.managers.systemview.walker.MetastorageViewWalker;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.MetastorageView;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERF_STAT_DIR;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.writeString;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests performance start with system views.
 */
public class PerformanceStatisticsSystemViewTest extends AbstractPerformanceStatisticsTest {
    /** */
    private static final List<String> IGNORED_VIEWS = List.of(
        "baseline.node.attributes",
        "node.attributes",
        "metrics",
        "caches",
        "sql.queries",
        "nodes");

    /** */
    private static final int VALID_VIEWS_CNT = 10;

    /** */
    private static final int INVALID_VIEWS_CNT = 10;

    /** Read buffer size. */
    private static final int BUFFER_SIZE = 100;

    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());
        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testSystemViewCaches() throws Exception {
        LogListener lsnr = LogListener.matches("Finished writing system views to performance statistics file:").build();
        listeningLog.registerListener(lsnr);

        try (IgniteEx igniteEx = startGrid(0)) {
            startCollectStatistics();

            Set<String> viewsExpected = new HashSet<>();
            igniteEx.context().systemView().forEach(view -> {
                if (view.size() > 0 && !IGNORED_VIEWS.contains(view.name()))
                    viewsExpected.add(view.name());
            });

            assertTrue("Performance statistics writer did not finish.", waitForCondition(lsnr::check, TIMEOUT));

            Set<String> viewsActual = new HashSet<>();
            stopCollectStatisticsAndRead(new TestHandler() {
                @Override public void systemView(UUID id, String name, List<String> schema, List<Object> row) {
                    viewsActual.add(name);
                }
            });

            assertEquals(1, systemViewStatisticsFiles(statisticsFiles()).size());
            assertEquals(viewsExpected, viewsActual);
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testInvalidSystemView() throws Exception {
        LogListener lsnr = LogListener.matches("Finished writing system views to performance statistics file:").build();
        listeningLog.registerListener(lsnr);

        LogListener warningLsnr = LogListener.matches("Unable to write system view:").times(INVALID_VIEWS_CNT).build();
        listeningLog.registerListener(warningLsnr);

        try (IgniteEx igniteEx = startGrid(0)) {
            GridSystemViewManager sysViewMngr = igniteEx.context().systemView();

            Map<?, ?> oldViews = U.field(sysViewMngr, "systemViews");
            oldViews.clear();

            List<String> viewsToWrite = U.staticField(SystemViewFileWriter.class, "SYSTEM_VIEWS");

            Map<String, String> viewsExpected = new HashMap<>(VALID_VIEWS_CNT);

            for (int i = 0; i < VALID_VIEWS_CNT; i++) {
                String val = "value " + i;
                MetastorageView view = new MetastorageView("name", val);

                String viewName = "valid_" + i;
                sysViewMngr.registerView(
                    viewName,
                    "valid_desc",
                    new MetastorageViewWalker(), () -> Collections.singletonList(view), identity());

                viewsToWrite.add(viewName);

                viewsExpected.put(viewName, val);
            }

            for (int i = 0; i < INVALID_VIEWS_CNT; i++) {
                String viewName = "invalid_desc" + i;

                sysViewMngr.registerView(
                    viewName,
                    "invalid_" + i,
                    new MetastorageViewWalker(), () -> null, identity());

                viewsToWrite.add(viewName);
            }

            startCollectStatistics();

            assertTrue("Performance statistics writer did not catch exception.", waitForCondition(warningLsnr::check, TIMEOUT));
            assertTrue("Performance statistics writer did not finish.", waitForCondition(lsnr::check, TIMEOUT));

            Map<String, String> viewsActual = new HashMap<>();
            stopCollectStatisticsAndRead(new TestHandler() {
                @Override public void systemView(UUID id, String name, List<String> schema, List<Object> row) {
                    Object val = row.get(schema.indexOf("value"));
                    viewsActual.put(name, String.valueOf(val));
                }
            });

            assertEquals(1, systemViewStatisticsFiles(statisticsFiles()).size());
            assertEquals(viewsExpected, viewsActual);
        }
    }

    /** Tests empty system view with custom system view walker. */
    @Test
    public void testCustomEmptySystemView() throws Exception {
        File dir = U.resolveWorkDirectory(U.defaultWorkDirectory(), PERF_STAT_DIR, false);

        List<String> expRow = createStatistics(dir);

        FilePerformanceStatisticsReader reader = new FilePerformanceStatisticsReader(BUFFER_SIZE, new TestHandler() {
            @Override public void systemView(UUID id, String name, List<String> schema, List<Object> row) {
                assertEquals("metastorage", name);
                assertEquals(expRow, row);
            }
        });

        reader.read(List.of(dir));
    }

    /** Creates test performance statistics file. */
    private List<String> createStatistics(File dir) throws Exception {
        File file = new File(dir, "node-" + randomUUID() + ".prf");

        try (FileIO fileIo = new RandomAccessFileIOFactory().create(file)) {
            ByteBuffer buf = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());

            buf.put(OperationType.VERSION.id());
            buf.putShort(FilePerformanceStatisticsWriter.FILE_FORMAT_VERSION);

            writeSystemView(buf, "customView", "customWalker", null);

            List<String> row = List.of("key", "value");
            writeSystemView(buf, "metastorage", MetastorageViewWalker.class.getName(), row);

            buf.flip();

            fileIo.write(buf);

            fileIo.force();

            return row;
        }
    }

    /** */
    private void writeSystemView(ByteBuffer buf, String view, String walker, List<String> row) {
        buf.put(OperationType.SYSTEM_VIEW_SCHEMA.id());

        writeString(buf, view, false);
        writeString(buf, walker, false);

        if (row == null)
            return;

        buf.put(OperationType.SYSTEM_VIEW_ROW.id());

        for (String val : row)
            writeString(buf, val, false);
    }
}
