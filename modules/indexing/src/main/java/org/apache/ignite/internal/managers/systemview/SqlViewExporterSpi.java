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

package org.apache.ignite.internal.managers.systemview;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.systemview.view.SystemView;

import static org.apache.ignite.internal.processors.query.QueryUtils.SCHEMA_SYS;

/**
 * This SPI implementation exports metrics as SQL views.
 *
 * Note, instance of this class created with reflection.
 * @see GridSystemViewManager#SYSTEM_VIEW_SQL_SPI
 */
class SqlViewExporterSpi extends AbstractSystemViewExporterSpi {
    /** */
    private GridKernalContext ctx;

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        ctx = ((IgniteEx)ignite()).context();
        sysViewReg.forEach(this::register);
        sysViewReg.addSystemViewCreationListener(this::register);
    }

    /**
     * Registers system view as SQL View.
     *
     * @param sysView System view.
     */
    private void register(SystemView<?> sysView) {
        if (log.isDebugEnabled())
            log.debug("Found new system view [name=" + sysView.name() + ']');

        ctx.query().schemaManager().createSystemView(SCHEMA_SYS, sysView);
    }
}
