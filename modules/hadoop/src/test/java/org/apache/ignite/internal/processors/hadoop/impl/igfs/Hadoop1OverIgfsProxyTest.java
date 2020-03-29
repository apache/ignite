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

import java.io.OutputStream;
import java.util.Collection;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * DUAL_ASYNC mode test.
 */
public class Hadoop1OverIgfsProxyTest extends Hadoop1DualAbstractTest {
    /**
     * Constructor.
     */
    public Hadoop1OverIgfsProxyTest() {
        super(IgfsMode.PROXY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinity() throws Exception {
        long fileSize = 32L * 1024 * 1024;

        IgfsPath filePath = new IgfsPath("/file");

        try (OutputStream os = igfs.create(filePath, true)) {
            for(int i = 0; i < fileSize / chunk.length; ++i)
                os.write(chunk);
        }

        long len = igfs.info(filePath).length();
        int start = 0;

        // Check default maxLen (maxLen = 0)
        for (int i = 0; i < igfs.context().data().groupBlockSize() / 1024; i++) {
            Collection<IgfsBlockLocation> blocks = igfs.affinity(filePath, start, len);

            assertEquals(F.first(blocks).start(), start);
            assertEquals(start + len, F.last(blocks).start() + F.last(blocks).length());

            len -= 1024 * 2;
            start += 1024;
        }

    }

}
