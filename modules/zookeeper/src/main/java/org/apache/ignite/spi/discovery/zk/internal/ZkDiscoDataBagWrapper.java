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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.discovery.SerializableDataBagItemWrapper;

/** Data bag data holder. */
public class ZkDiscoDataBagWrapper implements Message {
    /** */
    @Order(0)
    Map<Integer, Message> data;

    /** */
    private IgniteCheckedException unmarshErr;

    /** Default constructor for {@link MessageFactory}. */
    public ZkDiscoDataBagWrapper() {
        // No-op.
    }

    /** @param data Discovery data. */
    public ZkDiscoDataBagWrapper(Map<Integer, Message> data) {
        this.data = data;
    }

    /**
     * Returns data or throws caught unmarshalling errors.
     *
     * @return Data.
     * @throws IgniteCheckedException Unmarshalling exception, if any.
     */
    public Map<Integer, Message> unmarshalledData() throws IgniteCheckedException {
        if (unmarshErr != null)
            throw unmarshErr;

        IgniteCheckedException err = null;

        for (Message msg : data.values()) {
            if (msg instanceof SerializableDataBagItemWrapper wrapper && wrapper.unmarshallError() != null) {
                IgniteCheckedException e = wrapper.unmarshallError();

                if (err == null)
                    err = e;
                else
                    err.addSuppressed(e);
            }
        }

        if (err != null) {
            unmarshErr = err;

            throw err;
        }

        return data;
    }
}
