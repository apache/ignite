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

package org.apache.ignite.internal.processors.service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Service single node deployment result.
 * <p/>
 * Contains count of deployed service instances on single node and deployment errors if exist.
 */
public class ServiceSingleNodeDeploymentResult implements MarshallableMessage, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Count of service's instances. */
    @Order(0)
    int cnt;

    /** Exceptions. */
    private @Nullable Collection<Throwable> errors;

    /** Serialized {@link #errors}. */
    @Order(1)
    @Nullable Collection<byte[]> errorsBytes;

    /** Logger. */
    private IgniteLogger log;

    /**
     * Empty constructor for marshalling purposes.
     */
    public ServiceSingleNodeDeploymentResult() {
    }

    /**
     * @param cnt Count of service's instances.
     * @param log Logger.
     */
    public ServiceSingleNodeDeploymentResult(int cnt, IgniteLogger log) {
        this.cnt = cnt;
        this.log = log;
    }

    /**
     * @return Count of service's instances.
     */
    public int count() {
        return cnt;
    }

    /**
     * @return Exceptions.
     */
    public @NotNull Collection<Throwable> errors() {
        return F.emptyIfNull(errors);
    }

    /**
     * @param errors Exceptions.
     */
    public void errors(@Nullable Collection<Throwable> errors) {
        this.errors = errors;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (F.isEmpty(errors))
            return;

        errorsBytes = new ArrayList<>();

        for (Throwable th : errors) {
            try {
                byte[] arr = U.marshal(marsh, th);

                errorsBytes.add(arr);
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to marshal deployment error, err=" + th, e);

                try {
                    byte[] arr = U.marshal(
                        marsh,
                        new IgniteCheckedException("Failed to marshal deployment error, see server logs for details, err=" + th)
                    );

                    errorsBytes.add(arr);
                }
                catch (IgniteCheckedException ex) {
                    log.error("Failed to attach deployment error information to deployment result message", ex);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        try {
            if (errorsBytes != null && errors == null) {
                errors = new ArrayList<>();

                for (byte[] arr : errorsBytes)
                    errors.add(U.unmarshal(marsh, arr, clsLdr));
            }
        }
        catch (IgniteCheckedException e) {
            U.error(null, "Failed to unmarshal deployment result message", e);

            errors.add(new IgniteCheckedException("Failed to unmarshal deployment error, see server logs for details."));
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceSingleNodeDeploymentResult.class, this);
    }
}
