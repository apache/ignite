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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERF_STAT_DIR;
import static org.apache.ignite.internal.processors.performancestatistics.Utils.writeString;
import static org.apache.ignite.internal.processors.performancestatistics.Utils.writeUuid;

/**
 * Tests forward read mode for {@link OperationType#QUERY_PROPERTY} records.
 */
public class ForwardReadQueryPropertyTest extends AbstractPerformanceStatisticsTest {
    /** Read buffer size. */
    private static final int BUFFER_SIZE = 100;

    /** @throws Exception If failed. */
    @Test
    public void testStringForwardRead() throws Exception {
        File dir = U.resolveWorkDirectory(U.defaultWorkDirectory(), PERF_STAT_DIR, false);

        Map<String, String> expProps = createStatistics(dir);
        Map<String, String> actualProps = new HashMap<>();

        new FilePerformanceStatisticsReader(BUFFER_SIZE, new TestHandler() {
            @Override public void queryProperty(UUID nodeId, GridCacheQueryType type, UUID qryNodeId, long id, String name,
                String val) {
                assertNotNull(name);
                assertNotNull(val);

                actualProps.put(name, val);
            }
        }).read(singletonList(dir));

        assertEquals(expProps, actualProps);
    }

    /** Creates test performance statistics file. */
    private Map<String, String> createStatistics(File dir) throws Exception {
        File file = new File(dir, "node-" + randomUUID() + ".prf");

        try (FileIO fileIo = new RandomAccessFileIOFactory().create(file)) {
            ByteBuffer buf = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());

            buf.put(OperationType.VERSION.id());
            buf.putShort(FilePerformanceStatisticsWriter.FILE_FORMAT_VERSION);

            writeQueryProperty(buf, "property", true, "val", true);
            writeQueryProperty(buf, "property", false, "val", false);
            writeQueryProperty(buf, "propertyNan", true, "valNan", true);

            buf.flip();

            fileIo.write(buf);

            fileIo.force();
        }

        return Map.of("property", "val");
    }

    /** */
    private static void writeQueryProperty(ByteBuffer buf, String name, boolean nameCached, String val, boolean valCached) {
        buf.put(OperationType.QUERY_PROPERTY.id());

        writeString(buf, name, nameCached);
        writeString(buf, val, valCached);

        buf.put((byte)GridCacheQueryType.SQL_FIELDS.ordinal());

        writeUuid(buf, randomUUID());

        buf.putLong(0);
    }
}
