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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** Wrapper message for serializable data. */
public class ObjectData implements MarshallableMessage {
    /** */
    @GridToStringInclude
    private Serializable data;

    /** */
    @GridToStringExclude
    @Order(0)
    byte[] dataBytes;

    /** */
    public ObjectData() {}

    /**
     * @param data Original data.
     */
    public ObjectData(Serializable data) {
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
            data = U.unmarshal(marsh, dataBytes, clsLdr);

            dataBytes = null;
        }
    }

    /**
     * @param msg Message.
     * @param <T> Type of data.
     *
     * @return Original data unwrapped from a message.
     */
    public static <T> T unwrap(@Nullable Message msg) {
        return msg != null ? (T)(((ObjectData)msg).data) : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ObjectData.class, this);
    }
}
