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

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;

/**
 * IGFS Hadoop file system IPC loopback self test in DUAL_SYNC mode.
 */
public class IgniteHadoopFileSystemLoopbackEmbeddedDualSyncSelfTest
    extends IgniteHadoopFileSystemLoopbackAbstractSelfTest {
    /**
     * Constructor.
     */
    public IgniteHadoopFileSystemLoopbackEmbeddedDualSyncSelfTest() {
        super(DUAL_SYNC, false);
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9919")
    @Test
    @Override public void testRenameIfSrcPathIsAlreadyBeingOpenedToRead() throws Exception {
        super.testRenameIfSrcPathIsAlreadyBeingOpenedToRead();
    }
}
