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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.stream.Stream;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.configuration.distributed.DistributePropertyListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.processors.query.DistributedSqlConfiguration;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.setDefaultValue;

/** Distributed Calcite-engine configuration. */
public class DistributedCalciteConfiguration extends DistributedSqlConfiguration {
    /** Globally disabled rules property name. */
    public static final String DISABLED_RULES_PROPERTY_NAME = "sql.calcite.disabledRules";

    /** Default value of the disabled rules. */
    public static final String[] DFLT_DISABLED_RULES = new String[0];

    /** Globally disabled rules. */
    private volatile DistributedChangeableProperty<String[]> disabledRules;

    /** */
    private final QueryPlanCache qryPlanCache;

    /** */
    public DistributedCalciteConfiguration(GridKernalContext ctx, QueryPlanCache qryPlanCache, IgniteLogger log) {
        super(ctx, log);

        this.qryPlanCache = qryPlanCache;
    }

    /**
     * @return Globally disabled planning rules.
     * @see #DISABLED_RULES_PROPERTY_NAME
     */
    public String[] disabledRules() {
        DistributedChangeableProperty<String[]> disabledRules = this.disabledRules;

        String[] res = disabledRules == null ? DFLT_DISABLED_RULES : disabledRules.get();

        return res != null ? res : DFLT_DISABLED_RULES;
    }

    /** {@inheritDoc} */
    @Override protected void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
        super.onReadyToRegister(dispatcher);

        registerProperty(
            dispatcher,
            DISABLED_RULES_PROPERTY_NAME,
            prop -> disabledRules = prop,
            () -> new SimpleDistributedProperty<>(
                DISABLED_RULES_PROPERTY_NAME,
                str -> Stream.of(str.split(",")).map(String::trim).filter(s -> !s.isBlank()).toArray(String[]::new),
                "Comma-separated list of Calcite's disabled planning rules. NOTE: cleans the planning cache on cnage."
            ),
            log
        );

        disabledRules.addListener(new DistributePropertyListener<>() {
            @Override public void onUpdate(String name, String[] oldVal, String[] newVal) {
                if (oldVal != null && F.compareArrays(oldVal, newVal) != 0) {
                    log.warning("Cleaning Calcite's cache plan by setting changing of the property '"
                        + DISABLED_RULES_PROPERTY_NAME + "'.");

                    qryPlanCache.clear();
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected void onReadyToWrite() {
        super.onReadyToWrite();

        setDefaultValue(disabledRules, DFLT_DISABLED_RULES, log);
    }

    /** */
    DistributedChangeableProperty<String[]> disabledRulesProperty() {
        return disabledRules;
    }
}
