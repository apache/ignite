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
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.processors.query.DistributedSqlConfiguration;

import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.setDefaultValue;

/** Distributed Calcite-engine configuration. */
public class DistributedCalciteConfiguration extends DistributedSqlConfiguration {
    /** Globally disabled rules property name. */
    public static final String DISABLED_RULES_PROPERTY_NAME = "sql.calcite.disabledRules";

    /** Default value of the disabled rules. */
    public static final String[] DFLT_DISABLED_RULES = new String[0];

    /** Query timeout. */
    private volatile DistributedChangeableProperty<String[]> disabledRules;

    /** */
    public DistributedCalciteConfiguration(GridKernalContext ctx, IgniteLogger log) {
        super(ctx, log);
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
                str -> Stream.of(str.split(",")).map(String::trim).toArray(String[]::new),
                "Comma-separated list of Calcite's disabled planning rules. NOTE: cleans the planning cache!"
            ),
            log
        );
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
