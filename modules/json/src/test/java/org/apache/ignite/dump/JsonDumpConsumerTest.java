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

package org.apache.ignite.dump;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.DumpConsumerKernalContextAware;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.IgniteCacheDumpSelfTest;

import java.util.Iterator;
import java.util.Set;

/** TODO: FIXME */
public class JsonDumpConsumerTest extends IgniteCacheDumpSelfTest {
    /** {@inheritDoc} */
    @Override protected TestDumpConsumer dumpConsumer(Set<String> expectedFoundCaches, int expectedDfltDumpSz, int expectedGrpDumpSz, int expectedCnt) {
        return new TestJsonDumpConsumer();
    }

    public static class TestJsonDumpConsumer extends TestDumpConsumer implements DumpConsumerKernalContextAware {
        /** */
        private final JsonDumpConsumer jsonDumpConsumer = new JsonDumpConsumer();

        /** {@inheritDoc} */
        @Override public void start(GridKernalContext ctx) {
            jsonDumpConsumer.start(ctx);
            super.start();
        }

        /** {@inheritDoc} */
        @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
            jsonDumpConsumer.onPartition(grp, part, data);
        }
    }
}
