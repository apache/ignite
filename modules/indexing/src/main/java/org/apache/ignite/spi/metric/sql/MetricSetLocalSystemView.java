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

package org.apache.ignite.spi.metric.sql;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.MetricRegistry;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlAbstractLocalSystemView;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewColumnCondition;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * SQL view representing content of metric set.
 */
public class MetricSetLocalSystemView extends SqlAbstractLocalSystemView {
    /**
     * Metric set name.
     */
    private String msetName;

    /**
     * Metric set.
     */
    private MetricRegistry mreg;

    /**
     * @param set Metric set.
     * @param ctx Context.
     */
    public MetricSetLocalSystemView(String msetName, MetricRegistry mreg, GridKernalContext ctx) {
        super("METRIC_SET_" + msetName.toUpperCase().replace('-', '_').replace('.', '_'),
            "Metric set " + msetName,
            ctx,
            newColumn("NAME", Value.STRING),
            newColumn("VALUE", Value.STRING),
            newColumn("DESCRIPTION", Value.STRING));

        this.msetName = msetName;
        this.mreg = mreg.withPrefix(msetName);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition nameCond = conditionForColumn("NAME", first, last);

        Collection<Metric> metrics;

        if (nameCond.isEquality()) {
            Metric metric = mreg.findMetric(nameCond.valueForEquality().getString());

            metrics = metric == null ? Collections.emptySet() : Collections.singleton(metric);
        }
        else
            metrics = mreg.getMetrics();

        return F.iterator(metrics,
            m -> createRow(ses,
                m.name().substring(msetName.length() + 1),
                m.getAsString(),
                m.description()),
            true);
    }
}
