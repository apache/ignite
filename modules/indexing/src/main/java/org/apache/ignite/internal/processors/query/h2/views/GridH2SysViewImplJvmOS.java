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
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: JVM runtime.
 */
public class GridH2SysViewImplJvmOS extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplJvmOS(GridKernalContext ctx) {
        super("JVM_OS", "JVM operating system", ctx,
            newColumn("OS_NAME"),
            newColumn("OS_VERSION"),
            newColumn("ARCH"),
            newColumn("AVAILABLE_PROCESSORS", Value.INT),
            newColumn("SYSTEM_LOAD_AVG", Value.DOUBLE)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>(1);

        OperatingSystemMXBean mxBean = ManagementFactory.getOperatingSystemMXBean();

        rows.add(
            createRow(ses, 1L,
                mxBean.getName(),
                mxBean.getVersion(),
                mxBean.getArch(),
                mxBean.getAvailableProcessors(),
                mxBean.getSystemLoadAverage()
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
        return 1;
    }
}
