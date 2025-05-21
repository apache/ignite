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

package org.apache.ignite.testsuites;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWALTailIsReachedDuringIterationOverArchiveTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFormatFileFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalIteratorExceptionDuringReadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.reader.IgniteWalReaderTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneWalRecordsIteratorTest;
import org.junit.BeforeClass;

import static org.apache.ignite.testsuites.IgnitePdsCompressionTestSuite.enableCompressionByDefault;

/**
 * Supports tests from {@code IgnitePdsTestSuite2} that need special treatment for page size per IGNITE-10431.
 */
class SpecificPageSizeTests {
    /** */
    static List<Class> testsToIgnore() {
        return Arrays.asList(IgniteWALTailIsReachedDuringIterationOverArchiveTest.class,
            StandaloneWalRecordsIteratorTest.class, IgniteWalFormatFileFailoverTest.class,
            IgniteWalIteratorExceptionDuringReadTest.class, IgniteWalReaderTest.class);
    }

    /** */
    static List<Class<?>> testsToUse() {
        return Arrays.asList(IgniteWALTailIsReachedDuringIterationOverArchiveCompressTest.class,
            StandaloneWalRecordsIteratorCompressTest.class, IgniteWalFormatFileFailoverCompressTest.class,
            IgniteWalIteratorExceptionDuringReadCompressTest.class, IgniteWalReaderCompressTest.class);
    }

    /** */
    public static class IgniteWALTailIsReachedDuringIterationOverArchiveCompressTest
        extends IgniteWALTailIsReachedDuringIterationOverArchiveTest {
        /** */
        @BeforeClass
        public static void init() {
            enableCompressionByDefault();
        }
    }

    /** */
    public static class StandaloneWalRecordsIteratorCompressTest
        extends StandaloneWalRecordsIteratorTest {
        /** */
        @BeforeClass
        public static void init() {
            enableCompressionByDefault();
        }
    }

    /** */
    public static class IgniteWalFormatFileFailoverCompressTest
        extends IgniteWalFormatFileFailoverTest {
        /** */
        @BeforeClass
        public static void init() {
            enableCompressionByDefault();
        }
    }

    /** */
    public static class IgniteWalIteratorExceptionDuringReadCompressTest
        extends IgniteWalIteratorExceptionDuringReadTest {
        /** */
        @BeforeClass
        public static void init() {
            enableCompressionByDefault();
        }
    }

    /** */
    public static class IgniteWalReaderCompressTest
        extends IgniteWalReaderTest {
        /** */
        @BeforeClass
        public static void init() {
            enableCompressionByDefault();
        }
    }
}
