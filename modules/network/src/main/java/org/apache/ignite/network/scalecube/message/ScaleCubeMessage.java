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

package org.apache.ignite.network.scalecube.message;

import io.scalecube.cluster.transport.api.Message;
import java.util.Map;
import org.apache.ignite.network.message.NetworkMessage;

/**
 * Wrapper for ScaleCube's {@link Message}.
 * {@link Message#data} is stored in {@link #array} and {@link Message#headers} are stored in {@link #headers}.
 */
public class ScaleCubeMessage implements NetworkMessage {
    /** Direct type. */
    public static final short TYPE = 1;

    /** Message's data. */
    private final byte[] array;

    /** Message's headers. */
    private final Map<String, String> headers;

    /** Constructor. */
    public ScaleCubeMessage(byte[] array, Map<String, String> headers) {
        this.array = array;
        this.headers = headers;
    }

    /**
     * @return Message's data.
     */
    public byte[] getArray() {
        return array;
    }

    /**
     * @return Message's headers.
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE;
    }
}
