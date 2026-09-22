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

package org.apache.ignite.internal.processors.query;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.A;

import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.makeUpdateListener;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.setDefaultValue;

/**
 * Common distributed SQL configuration.
 */
public abstract class DistributedSqlConfiguration {
    /** */
    private static final String QUERY_TIMEOUT_PROPERTY_NAME = "sql.defaultQueryTimeout";

    /** Property update message. */
    protected static final String PROPERTY_UPDATE_MESSAGE =
        "SQL parameter '%s' was changed from '%s' to '%s'";

    /** Default value of the query timeout. */
    public static final int DFLT_QRY_TIMEOUT = 0;

    /** */
    protected final GridKernalContext ctx;

    /** */
    protected final IgniteLogger log;

    /** Query timeout. */
    private volatile DistributedChangeableProperty<Integer> dfltQryTimeout;

    /**
     * @param ctx Kernal context
     * @param log Logger.
     */
    protected DistributedSqlConfiguration(GridKernalContext ctx, IgniteLogger log) {
        this.ctx = ctx;
        this.log = log;

        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    DistributedSqlConfiguration.this.onReadyToRegister(dispatcher);
                }

                @Override public void onReadyToWrite() {
                    DistributedSqlConfiguration.this.onReadyToWrite();
                }
            }
        );
    }

    /** */
    protected void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
        registerProperty(
            dispatcher,
            QUERY_TIMEOUT_PROPERTY_NAME,
            prop -> dfltQryTimeout = prop,
            () -> new SimpleDistributedProperty<>(
                QUERY_TIMEOUT_PROPERTY_NAME,
                SimpleDistributedProperty::parseNonNegativeInteger,
                "Timeout in milliseconds for default query timeout. 0 means there is no timeout."
            ),
            log
        );
    }

    /** */
    protected void onReadyToWrite() {
        setDefaultValue(dfltQryTimeout, (int)ctx.config().getSqlConfiguration().getDefaultQueryTimeout(), log);
    }

    /** */
    protected static <T extends Serializable> void registerProperty(
        DistributedPropertyDispatcher dispatcher,
        String propName,
        Consumer<DistributedChangeableProperty<T>> propSetter,
        Supplier<DistributedChangeableProperty<T>> newPropCreator,
        IgniteLogger log
    ) {
        DistributedChangeableProperty<T> prop = dispatcher.property(propName);

        if (prop == null) {
            prop = newPropCreator.get();

            prop.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));

            dispatcher.registerProperty(prop);
        }

        propSetter.accept(prop);
    }

    /**
     * @return Default query timeout.
     */
    public int defaultQueryTimeout() {
        Integer t = dfltQryTimeout == null ? null : dfltQryTimeout.get();

        return t != null ? t : DFLT_QRY_TIMEOUT;
    }

    /**
     * @param timeout Default query timeout.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> defaultQueryTimeout(int timeout) throws IgniteCheckedException {
        A.ensure(timeout >= 0,
            "default query timeout value must not be negative.");

        if (dfltQryTimeout == null)
            throw new IgniteCheckedException("Property " + QUERY_TIMEOUT_PROPERTY_NAME + " is not registered yet");

        return dfltQryTimeout.propagateAsync(timeout);
    }
}
