/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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

import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * IGFS Hadoop file system IPC loopback self test in SECONDARY mode.
 */
public class IgniteHadoopFileSystemLoopbackEmbeddedSecondarySelfTest extends
    IgniteHadoopFileSystemLoopbackAbstractSelfTest {
    /**
     * Constructor.
     */
    public IgniteHadoopFileSystemLoopbackEmbeddedSecondarySelfTest() {
        super(PROXY, false);
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9919")
    @Test
    @Override public void testRenameIfSrcPathIsAlreadyBeingOpenedToRead() throws Exception {
        super.testRenameIfSrcPathIsAlreadyBeingOpenedToRead();
    }
}
