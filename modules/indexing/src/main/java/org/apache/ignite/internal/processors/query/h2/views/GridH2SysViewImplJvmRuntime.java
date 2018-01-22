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

package org.apache.ignite.internal.processors.query.h2.views;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: JVM runtime.
 */
public class GridH2SysViewImplJvmRuntime extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplJvmRuntime(GridKernalContext ctx) {
        super("JVM_RUNTIME", "JVM runtime", ctx,
            newColumn("PID", Value.INT),
            newColumn("NAME"),
            newColumn("UPTIME", Value.TIME),
            newColumn("START_TIME", Value.TIMESTAMP),
            newColumn("JVM_IMPL_NAME"),
            newColumn("JVM_IMPL_VENDOR"),
            newColumn("JVM_IMPL_VERSION"),
            newColumn("JVM_SPEC_VERSION"),
            newColumn("INPUT_ARGUMENTS"),
            newColumn("CLASS_PATH"),
            newColumn("LIBRARY_PATH")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        RuntimeMXBean mxBean = ManagementFactory.getRuntimeMXBean();

        Collection<Row> rows = Collections.singleton(
            createRow(ses, 1L,
                U.jvmPid(),
                mxBean.getName(),
                valueTimeFromMillis(mxBean.getUptime()),
                valueTimestampFromMillis(mxBean.getStartTime()),
                mxBean.getVmName(),
                mxBean.getVmVendor(),
                mxBean.getVmVersion(),
                mxBean.getSpecVersion(),
                mxBean.getInputArguments(),
                mxBean.getClassPath(),
                mxBean.getLibraryPath()
            )
        );

        return rows;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return 1L;
    }
}
