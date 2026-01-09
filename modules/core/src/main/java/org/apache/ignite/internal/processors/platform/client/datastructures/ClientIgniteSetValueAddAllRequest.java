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

package org.apache.ignite.internal.processors.platform.client.datastructures;

import java.util.List;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.processors.platform.client.ClientBooleanResponse;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Set addAll request.
 */
public class ClientIgniteSetValueAddAllRequest extends ClientIgniteSetKeysRequest {
    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientIgniteSetValueAddAllRequest(BinaryReaderEx reader) {
        super(reader);
    }

    /** {@inheritDoc} */
    @Override ClientResponse process(IgniteSet<Object> set, List<Object> keys) {
        return new ClientBooleanResponse(requestId(), set.addAll(keys));
    }
}
