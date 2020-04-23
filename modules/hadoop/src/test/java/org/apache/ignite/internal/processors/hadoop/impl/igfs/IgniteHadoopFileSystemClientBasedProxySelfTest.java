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

import org.apache.ignite.igfs.IgfsMode;
import org.junit.Ignore;
import org.junit.Test;

/**
 * IGFS Hadoop file system Ignite client -based self test for PROXY mode.
 */
public class IgniteHadoopFileSystemClientBasedProxySelfTest extends IgniteHadoopFileSystemClientBasedAbstractSelfTest {
    /**
     * Constructor.
     */
    public IgniteHadoopFileSystemClientBasedProxySelfTest() {
        super(IgfsMode.PROXY);
    }

    /** {@inheritDoc} */
    @Override protected String getClientConfig() {
        return "modules/hadoop/src/test/config/igfs-cli-config-proxy.xml";
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9919")
    @Test
    @Override public void testRenameIfSrcPathIsAlreadyBeingOpenedToRead() throws Exception {
        super.testRenameIfSrcPathIsAlreadyBeingOpenedToRead();
    }
}
