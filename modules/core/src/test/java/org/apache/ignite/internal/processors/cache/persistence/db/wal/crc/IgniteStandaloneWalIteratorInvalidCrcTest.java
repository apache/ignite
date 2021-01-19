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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.crc;

import java.io.File;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class IgniteStandaloneWalIteratorInvalidCrcTest extends IgniteAbstractWalIteratorInvalidCrcTest {
    /** {@inheritDoc} */
    @NotNull @Override protected WALMode getWalMode() {
        return WALMode.LOG_ONLY;
    }

    /** {@inheritDoc} */
    @Override protected WALIterator getWalIterator(
        IgniteWriteAheadLogManager walMgr,
        boolean ignoreArchiveDir
    ) throws IgniteCheckedException {
        File walArchiveDir = U.field(walMgr, "walArchiveDir");
        File walDir = U.field(walMgr, "walWorkDir");

        IgniteWalIteratorFactory iterFactory = new IgniteWalIteratorFactory();

        return ignoreArchiveDir ? iterFactory.iterator(walDir) : iterFactory.iterator(walArchiveDir, walDir);
    }
}
