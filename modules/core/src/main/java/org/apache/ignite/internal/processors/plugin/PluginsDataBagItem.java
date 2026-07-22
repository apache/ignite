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

package org.apache.ignite.internal.processors.plugin;

import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/** */
public class PluginsDataBagItem implements MarshallableMessage {
    /** Original plugins data. */
    @Nullable Map<String, Serializable> data;

    /** Serialized plugins data. */
    @Order(0)
    @Nullable byte[] dataBytes;

    /** */
    public PluginsDataBagItem() { }

    /** @param data Plugins data. */
    public PluginsDataBagItem(@Nullable Map<String, Serializable> data) {
        this.data = data;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (!F.isEmpty(data))
            dataBytes = U.marshal(marsh, data);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        if (dataBytes != null)
            data = U.unmarshal(marsh, dataBytes, clsLdr);
    }

    /** @return Original plugins data. */
    public @Nullable Map<String, Serializable> data() {
        return data;
    }
}
