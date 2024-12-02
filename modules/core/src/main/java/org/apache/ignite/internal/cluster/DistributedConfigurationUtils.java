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

package org.apache.ignite.internal.cluster;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.configuration.distributed.DistributePropertyListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedProperty;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.jetbrains.annotations.NotNull;

import static java.lang.String.format;

/**
 * Distributed configuration utilities methods.
 */
public final class DistributedConfigurationUtils {
    /**
     */
    private DistributedConfigurationUtils() {
        // No-op.
    }

    /**
     * @param prop Property which value should be set.
     * @param val Default value.
     * @param log Logger.
     * @param <T> Property type.
     *
     * @return Future for the operation.
     */
    public static <T extends Serializable> IgniteInternalFuture<Void> setDefaultValue(
        DistributedProperty<T> prop,
        T val,
        IgniteLogger log
    ) {
        if (prop.get() == null) {
            try {
                IgniteInternalFuture<Void> fut = (IgniteInternalFuture<Void>)prop.propagateAsync(null, val);

                fut.listen(future -> {
                    if (future.error() != null)
                        log.error("Cannot set default value of '" + prop.getName() + '\'', future.error());
                });

                return fut;
            }
            catch (IgniteCheckedException e) {
                String errMsg = "Cannot initiate setting default value of '" + prop.getName() + '\'';

                log.error(errMsg, e);

                return new GridFinishedFuture<>(new IgniteCheckedException(errMsg, e));
            }
        }
        else {
            if (log.isDebugEnabled()) {
                log.debug("Skip set default value for distributed property [name=" + prop.getName() +
                    ", clusterValue=" + prop.get() + ", defaultValue=" + val + ']');
            }

            return new GridFinishedFuture<>();
        }
    }

    /**
     * @param propUpdMsg Update message.
     * @param log Logger.
     * @param <T> Type of property value.
     * @return Update property listener.
     */
    @NotNull public static <T> DistributePropertyListener<T> makeUpdateListener(String propUpdMsg, IgniteLogger log) {
        return (name, oldVal, newVal) -> {
            if (!Objects.equals(oldVal, newVal)) {
                if (log.isInfoEnabled())
                    log.info(format(propUpdMsg, name, oldVal, newVal));
            }
        };
    }
}
