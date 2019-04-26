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

package org.apache.ignite.internal.processors.maintain;

import java.io.File;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.mxbean.DebugMXBean;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class DebugMXBeanImpl implements DebugMXBean {

    private final DebugProcessor debug;

    /**
     * @param ctx Context.
     */
    public DebugMXBeanImpl(GridKernalContext ctx) {
        debug = ctx.debug();
    }

    @Override public void dumpPageHistory(boolean dumpToFile, boolean dumpToLog, String filePath, long... pageIds) {
        DebugProcessor.DebugPageBuilder builder = new DebugProcessor.DebugPageBuilder()
            .pageIds(pageIds);

        if (filePath != null)
            builder.fileOrFolderForDump(new File(filePath));

        if (dumpToFile)
            builder.addAction(DebugProcessor.DebugAction.PRINT_TO_FILE);

        if (dumpToFile)
            builder.addAction(DebugProcessor.DebugAction.PRINT_TO_LOG);

        try {
            debug.dumpPageHistory(builder);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
