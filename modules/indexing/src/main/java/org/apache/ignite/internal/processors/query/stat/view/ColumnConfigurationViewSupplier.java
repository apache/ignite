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

package org.apache.ignite.internal.processors.query.stat.view;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnConfigurationViewWalker;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsConfigurationManager;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Column configuration view supplier.
 */
public class ColumnConfigurationViewSupplier {
    /** Configuration manager. */
    IgniteStatisticsConfigurationManager cfgMgr;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param cfgMgr Configuration manager.
     * @param logSupplier Log supplier.
     */
    public ColumnConfigurationViewSupplier(
        IgniteStatisticsConfigurationManager cfgMgr,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.cfgMgr = cfgMgr;
        log = logSupplier.apply(ColumnConfigurationViewSupplier.class);
    }

    /**
     * Statistics column configuration view filterable supplier.
     *
     * @param filter Filter.
     * @return Iterable with selected statistics column configuration views.
     */
    public Iterable<StatisticsColumnConfigurationView> columnConfigurationViewSupplier(Map<String, Object> filter) {
        String schema = (String)filter.get(StatisticsColumnConfigurationViewWalker.SCHEMA_FILTER);
        String name = (String)filter.get(StatisticsColumnConfigurationViewWalker.NAME_FILTER);

        Collection<StatisticsObjectConfiguration> configs;

        try {
            if (!F.isEmpty(schema) && !F.isEmpty(name)) {
                StatisticsKey key = new StatisticsKey(schema, name);
                StatisticsObjectConfiguration keyCfg = cfgMgr.config(key);

                if (keyCfg == null)
                    return Collections.emptyList();

                configs = Collections.singletonList(keyCfg);
            }
            else
                configs = cfgMgr.getAllConfig();
        }
        catch (IgniteCheckedException e) {
            log.warning("Error while getting statistics configuration: " + e.getMessage(), e);

            configs = Collections.emptyList();
        }

        List<StatisticsColumnConfigurationView> res = new ArrayList<>();

        for (StatisticsObjectConfiguration cfg : configs) {
            for (StatisticsColumnConfiguration colCfg : cfg.columnsAll().values()) {
                if (!colCfg.tombstone())
                    res.add(new StatisticsColumnConfigurationView(cfg, colCfg));
            }
        }

        return res;
    }
}
