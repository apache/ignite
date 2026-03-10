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

package org.apache.ignite.internal;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Task session request.
 */
public class GridTaskSessionRequest implements Message {
    /** Task session ID. */
    @Order(0)
    IgniteUuid sesId;

    /** ID of job within a task. */
    @Order(1)
    IgniteUuid jobId;

    /** Changed attributes bytes. */
    @Order(2)
    byte[] attrsBytes;

    /** Changed attributes. */
    private Map<?, ?> attrs;

    /**
     * Empty constructor.
     */
    public GridTaskSessionRequest() {
        // No-op.
    }

    /**
     * @param sesId Session ID.
     * @param jobId Job ID.
     * @param attrs Attributes.
     */
    public GridTaskSessionRequest(IgniteUuid sesId, IgniteUuid jobId, Map<?, ?> attrs) {
        assert sesId != null;
        assert attrs != null;

        this.sesId = sesId;
        this.jobId = jobId;
        this.attrs = attrs;
    }

    /**
     * @return Changed attributes.
     */
    public Map<?, ?> attributes() {
        return attrs;
    }

    /**
     * @return Task session ID.
     */
    public IgniteUuid sessionId() {
        return sesId;
    }

    /**
     * @return Job ID.
     */
    public IgniteUuid jobId() {
        return jobId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 6;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTaskSessionRequest.class, this);
    }

    /**
     * Marshals changed attributes to byte array.
     *
     * @param marsh Marshaller.
     */
    public void marshalAttributes(Marshaller marsh) throws IgniteCheckedException {
        attrsBytes = U.marshal(marsh, attrs);
    }

    /**
     * Unmarshals changed attributes from byte array.
     *
     * @param marsh Marshaller.
     * @param ldr Class loader.
     */
    public void unmarshalAttributes(Marshaller marsh, ClassLoader ldr) throws IgniteCheckedException {
        if (attrsBytes != null) {
            attrs = U.unmarshal(marsh, attrsBytes, ldr);

            // It is not required anymore.
            attrsBytes = null;
        }
    }
}
