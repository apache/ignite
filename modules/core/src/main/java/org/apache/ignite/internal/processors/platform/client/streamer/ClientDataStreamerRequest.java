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

package org.apache.ignite.internal.processors.platform.client.streamer;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;

/**
 * Base class for streamer requests.
 */
abstract class ClientDataStreamerRequest extends ClientRequest {
    /**
     * Constructor.
     *
     * @param reader Data reader.
     */
    protected ClientDataStreamerRequest(BinaryRawReader reader) {
        super(reader);
    }

    /**
     * Returns invalid node state response.
     *
     * @return Invalid node state response.
     */
    protected ClientResponse getInvalidNodeStateResponse() {
        return new ClientResponse(requestId(), ClientStatus.INVALID_NODE_STATE,
                "Data streamer has been closed because node is stopping.");
    }
}
