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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
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

    /** Query timeout. */
    private volatile DistributedChangeableProperty<Integer> dfltQryTimeout;

    /**
     * @param ctx Kernal context
     * @param log Logger.
     */
    protected DistributedSqlConfiguration(
        GridKernalContext ctx,
        IgniteLogger log
    ) {
        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    DistributedChangeableProperty<Integer> prop = dispatcher.property(QUERY_TIMEOUT_PROPERTY_NAME);

                    if (prop != null) {
                        dfltQryTimeout = prop;

                        return;
                    }

                    dfltQryTimeout = new SimpleDistributedProperty<>(
                        QUERY_TIMEOUT_PROPERTY_NAME,
                        SimpleDistributedProperty::parseNonNegativeInteger
                    );

                    dfltQryTimeout.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));

                    dispatcher.registerProperties(dfltQryTimeout);
                }

                @SuppressWarnings("deprecation")
                @Override public void onReadyToWrite() {
                    if (ReadableDistributedMetaStorage.isSupported(ctx)) {
                        setDefaultValue(
                            dfltQryTimeout,
                            (int)ctx.config().getSqlConfiguration().getDefaultQueryTimeout(),
                            log);
                    }
                    else {
                        log.warning("Distributed metastorage is not supported. " +
                            "All distributed SQL configuration parameters are unavailable.");

                        // Set properties to default.
                        dfltQryTimeout.localUpdate((int)ctx.config().getSqlConfiguration().getDefaultQueryTimeout());
                    }
                }
            }
        );
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
