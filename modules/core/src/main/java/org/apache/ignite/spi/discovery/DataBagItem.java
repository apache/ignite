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

package org.apache.ignite.spi.discovery;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.Ignition.localIgnite;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CONTINUOUS_PROC;

/** Wrapper message for serializable data in a {@link DiscoveryDataBag}. */
public class DataBagItem implements MarshallableMessage {
    /** */
    @GridToStringInclude
    private Serializable data;

    /** */
    @GridToStringExclude
    @Order(0)
    byte[] dataBytes;

    /** Component id. */
    @Order(1)
    byte cmpId;

    /** */
    public DataBagItem() {}

    /**
     * @param data Original data.
     */
    public DataBagItem(Serializable data) {
        this.data = data;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (data != null)
            dataBytes = U.marshal(marsh, data);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        if (dataBytes != null) {
            try {
                data = U.unmarshal(marsh, dataBytes, clsLdr);
            } catch (IgniteCheckedException e) {
                if (CONTINUOUS_PROC.ordinal() == cmpId && X.hasCause(e, ClassNotFoundException.class) &&
                    localIgnite().configuration().isClientMode()) {
                    U.warn(localIgnite().log(), "Failed to unmarshal continuous query remote filter on client node. Can be ignored.");
                }
                else if (cmpId < GridComponent.DiscoveryDataExchangeType.VALUES.length) {
                    throw new UnmarshallException("Failed to unmarshal discovery data for component: " +
                            GridComponent.DiscoveryDataExchangeType.VALUES[cmpId], e);
                }
                else {
                    throw new UnmarshallException("Failed to unmarshal discovery data." +
                        " Component " + cmpId + " is not found.", e);
                }
            }
        }
    }

    /**
     * @param <T> Type of data.
     *
     * @return Original data unwrapped from a message.
     */
    <T> T unwrap() {
        return (T)(data);
    }

    /**
     * @param msg Message.
     * @param <T> Type of data.
     *
     * @return Original message or data unwrapped from an DataBagItem wrapper.
     */
    static @Nullable <T> T unwrapIfNecessary(@Nullable Message msg) {
        if (msg == null)
            return null;

        return msg instanceof DataBagItem ? ((DataBagItem)msg).unwrap() : (T)msg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataBagItem.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;

        DataBagItem data1 = (DataBagItem)o;

        return Objects.equals(data, data1.data) || Arrays.equals(dataBytes, data1.dataBytes);
    }

    /** */
    public static class UnmarshallException extends IgniteException {
        /**
         * Creates new exception with given error message and optional nested exception.
         *
         * @param msg Error message.
         * @param cause Optional nested exception (can be {@code null}).
         */
        public UnmarshallException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}
