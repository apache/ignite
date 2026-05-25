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

import java.io.Serializable;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.configuration.distributed.DistributePropertyListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.processors.query.DistributedSqlConfiguration;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.LifecycleAware;
import org.apache.ignite.internal.processors.query.calcite.util.Service;

import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.setDefaultValue;

/** Distributed Calcite-engine configuration. */
public class DistributedCalciteConfiguration extends DistributedSqlConfiguration implements Service, LifecycleAware {
    /** Globally disabled rules property name. */
    public static final String DISABLED_RULES_PROPERTY_NAME = "sql.calcite.disabledRules";

    /** Plan cache size property name. */
    public static final String PLAN_CACHE_SIZE_PROPERTY_NAME = "sql.calcite.planCacheSize";

    /** Default value of the disabled rules. */
    public static final String[] DFLT_DISABLED_RULES = new String[0];

    /** Default value of plan cache size. */
    public static final int DFLT_PLAN_CACHE_SIZE = 1024;

    /** Globally disabled rules. */
    private volatile DistributedChangeableProperty<String[]> disabledRules;

    /** Plan cache size. */
    private volatile DistributedChangeableProperty<Integer> planCacheSize;

    /** */
    private QueryPlanCache qryPlanCache;

    /** */
    public DistributedCalciteConfiguration(GridKernalContext ctx, IgniteLogger log) {
        super(ctx, log);
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        CalciteQueryProcessor proc = Objects.requireNonNull(Commons.lookupComponent(ctx, CalciteQueryProcessor.class));

        assert proc != null;

        qryPlanCache = proc.queryPlanCache();
    }

    /** {@inheritDoc} */
    @Override public void onStop() {
        // No-op.
    }

    /**
     * @return Globally disabled planning rules.
     * @see #DISABLED_RULES_PROPERTY_NAME
     */
    public String[] disabledRules() {
        return getProperty(disabledRules, DFLT_DISABLED_RULES);
    }

    /**
     * @return Plan cache size.
     */
    public int planCacheSize() {
        return getProperty(planCacheSize, DFLT_PLAN_CACHE_SIZE);
    }

    /** */
    private <T extends Serializable> T getProperty(DistributedChangeableProperty<T> prop, T dflt) {
        T res = prop == null ? dflt : prop.get();

        return res != null ? res : dflt;
    }

    /** {@inheritDoc} */
    @Override protected void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
        super.onReadyToRegister(dispatcher);

        DistributePropertyListener<Object> planCacheCleaner = (name, oldVal, newVal) -> {
            if (oldVal != null && !oldVal.equals(newVal)) {
                if (qryPlanCache != null) {
                    if (log.isInfoEnabled())
                        log.info("Cleaning Calcite's cache plan by changing of the property '" + name + "'.");

                    qryPlanCache.clear();
                }
            }
        };

        registerProperty(
            dispatcher,
            DISABLED_RULES_PROPERTY_NAME,
            prop -> disabledRules = prop,
            () -> new SimpleDistributedProperty<>(
                DISABLED_RULES_PROPERTY_NAME,
                str -> Stream.of(str.split(",")).map(String::trim).filter(s -> !s.isBlank()).toArray(String[]::new),
                "Comma-separated list of Calcite's disabled planning rules. NOTE: cleans the planning cache on change."
            ),
            log
        );

        disabledRules.addListener(planCacheCleaner);

        registerProperty(
            dispatcher,
            PLAN_CACHE_SIZE_PROPERTY_NAME,
            prop -> planCacheSize = prop,
            () -> new SimpleDistributedProperty<>(
                PLAN_CACHE_SIZE_PROPERTY_NAME,
                Integer::parseInt,
                "Calcite's plan cache size. NOTE: cleans the planning cache on change."
            ),
            log
        );

        planCacheSize.addListener(planCacheCleaner);
    }

    /** {@inheritDoc} */
    @Override protected void onReadyToWrite() {
        super.onReadyToWrite();

        setDefaultValue(disabledRules, DFLT_DISABLED_RULES, log);
        setDefaultValue(planCacheSize, DFLT_PLAN_CACHE_SIZE, log);
    }
}
