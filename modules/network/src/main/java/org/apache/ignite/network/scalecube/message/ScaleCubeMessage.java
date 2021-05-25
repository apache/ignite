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

import java.util.Map;
import io.scalecube.cluster.transport.api.Message;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.processor.annotations.AutoSerializable;

/**
 * Wrapper for ScaleCube's {@link Message}.
 * {@link Message#data} is stored in {@link #array} and {@link Message#headers} are stored in {@link #headers}.
 */
@AutoSerializable(messageFactory = ScaleCubeMessageFactory.class)
public interface ScaleCubeMessage extends NetworkMessage {
    /** Direct type. */
    public static final short TYPE = 1;

    byte[] array();

    Map<String, String> headers();

    interface Builder {
        Builder array(byte[] array);

        Builder headers(Map<String, String> headers);

        ScaleCubeMessage build();
    }

    /** {@inheritDoc} */
    @Override public default short directType() {
        return TYPE;
    }
}
