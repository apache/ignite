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

package org.apache.ignite.internal.processors.query.h2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.configuration.distributed.DistributePropertyListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.processors.query.DistributedSqlConfiguration;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.makeUpdateListener;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.setDefaultValue;

/**
 * Distributed configuration of the indexing module.
 */
public class DistributedIndexingConfiguration extends DistributedSqlConfiguration {
    /** Default disabled SQL functions. */
    public static final HashSet<String> DFLT_DISABLED_FUNCS = (HashSet<String>)Arrays.stream(new String[] {
        "FILE_READ",
        "FILE_WRITE",
        "CSVWRITE",
        "CSVREAD",
        "MEMORY_FREE",
        "MEMORY_USED",
        "LOCK_MODE",
        "LINK_SCHEMA",
        "SESSION_ID",
        "CANCEL_SESSION"
    }).collect(Collectors.toSet());

    /** Disabled SQL functions. */
    private final SimpleDistributedProperty<HashSet<String>> disabledSqlFuncs = new SimpleDistributedProperty<>(
        "sql.disabledFunctions",
        SimpleDistributedProperty::parseStringSet,
        "List of disabled SQL functions. Specify comma-separated functions to set a value."
    );

    /**
     * Disable creation Lucene index for String value type by default.
     * See: 'H2TableDescriptor#luceneIdx'.
     */
    private final DistributedBooleanProperty disableCreateLuceneIndexForStringValueType =
        DistributedBooleanProperty.detachedBooleanProperty("sql.disableCreateLuceneIndexForStringValueType",
            "Flag to disable creation Lucene index for String value type by default.");

    /**
     * @param ctx Kernal context
     * @param log Logger.
     */
    public DistributedIndexingConfiguration(
        GridKernalContext ctx,
        IgniteLogger log
    ) {
        super(ctx, log);

        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    disabledSqlFuncs.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));

                    dispatcher.registerProperty(disabledSqlFuncs);
                    dispatcher.registerProperty(disableCreateLuceneIndexForStringValueType);
                }

                @Override public void onReadyToWrite() {
                    setDefaultValue(
                        disabledSqlFuncs,
                        DFLT_DISABLED_FUNCS,
                        log);

                    setDefaultValue(
                        disableCreateLuceneIndexForStringValueType,
                        false,
                        log);
                }
            }
        );
    }

    /**
     * @return Disabled SQL functions.
     */
    public Set<String> disabledFunctions() {
        Set<String> ret = disabledSqlFuncs.get();

        return ret != null ? ret : DFLT_DISABLED_FUNCS;
    }

    /**
     * @param disabledFuncs Set of disabled functions.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> disabledFunctions(HashSet<String> disabledFuncs)
        throws IgniteCheckedException {
        return disabledSqlFuncs.propagateAsync(disabledFuncs);
    }

    /** */
    public void listenDisabledFunctions(DistributePropertyListener<HashSet<String>> lsnr) {
        disabledSqlFuncs.addListener(lsnr);
    }

    /** */
    public boolean isDisableCreateLuceneIndexForStringValueType() {
        Boolean ret = disableCreateLuceneIndexForStringValueType.get();

        return ret != null && ret;
    }

    /** */
    public GridFutureAdapter<?> disableCreateLuceneIndexForStringValueType(boolean disableCreateIdx)
        throws IgniteCheckedException {
        return disableCreateLuceneIndexForStringValueType.propagateAsync(disableCreateIdx);
    }
}
