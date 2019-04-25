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

import org.apache.ignite.igfs.IgfsMode;
import org.junit.Ignore;
import org.junit.Test;

/**
 * IGFS Hadoop file system Ignite client -based self test for PRIMARY mode.
 */
public class IgniteHadoopFileSystemClientBasedPrimarySelfTest
    extends IgniteHadoopFileSystemClientBasedAbstractSelfTest {
    /**
     * Constructor.
     */
    public IgniteHadoopFileSystemClientBasedPrimarySelfTest() {
        super(IgfsMode.PRIMARY);
    }

    /** {@inheritDoc} */
    @Override protected String getClientConfig() {
        return "modules/hadoop/src/test/config/igfs-cli-config-primary.xml";
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9919")
    @Test
    @Override public void testRenameIfSrcPathIsAlreadyBeingOpenedToRead() throws Exception {
        super.testRenameIfSrcPathIsAlreadyBeingOpenedToRead();
    }
}
