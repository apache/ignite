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

package org.apache.ignite.internal.profiling;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.profiling.FileProfiling.DFLT_BUFFER_SIZE;
import static org.apache.ignite.internal.profiling.FileProfiling.DFLT_FILE_MAX_SIZE;
import static org.apache.ignite.internal.profiling.FileProfiling.DFLT_FLUSH_SIZE;
import static org.apache.ignite.internal.profiling.FileProfiling.PROFILING_DIR;
import static org.apache.ignite.internal.profiling.FileProfiling.profilingFile;

/**
 * Ignite profiling abstract test.
 */
public class AbstractProfilingTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), PROFILING_DIR, false));
    }

    /** Starts profiling. */
    public static void startProfiling() throws Exception {
        List<Ignite> grids = G.allGrids();

        assertFalse(grids.isEmpty());

        IgniteEx ignite = (IgniteEx)grids.get(0);

        ignite.context().metric().startProfiling(DFLT_FILE_MAX_SIZE, DFLT_BUFFER_SIZE, DFLT_FLUSH_SIZE);
    }

    /** Stops profiling and checks listeners on all grids. */
    public static void stopProfilingAndCheck(LogListener... lsnrs) throws Exception {
        List<Ignite> grids = G.allGrids();

        assertFalse(grids.isEmpty());

        IgniteEx ignite = (IgniteEx)grids.get(0);

        ignite.context().metric().stopProfiling().get();

        for (Ignite grid : grids)
            TestFileProfilingReader.readToLog(profilingFile(((IgniteEx)grid).context()).toPath(), grid.log());

        for (LogListener lsnr : lsnrs)
            assertTrue(lsnr.check());
    }
}
