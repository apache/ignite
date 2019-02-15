/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;

/**
 * Incomplete object.
 */
public class IncompleteObject<T> {
    /** */
    private long nextLink;

    /** */
    protected byte[] data;

    /** */
    private T obj;

    /** */
    private int off;

    /**
     * @param data Data bytes.
     */
    public IncompleteObject(byte[] data) {
        this.data = data;
    }

    /**
     * Constructor.
     */
    public IncompleteObject() {
        // No-op.
    }

    /**
     * @return Cache object if ready.
     */
    public T object() {
        return obj;
    }

    /**
     * @param obj Cache object.
     */
    public void object(T obj) {
        this.obj = obj;
    }

    /**
     * @return {@code True} if cache object is fully assembled.
     */
    public boolean isReady() {
        return data != null && off == data.length;
    }

    /**
     * @return Data array.
     */
    public byte[] data() {
        return data;
    }

    /**
     * @param buf Read remaining data.
     */
    public void readData(ByteBuffer buf) {
        assert data != null;

        final int len = Math.min(data.length - off, buf.remaining());

        buf.get(data, off, len);

        off += len;
    }

    /**
     * @return Next data page link for fragmented rows.
     */
    public long getNextLink() {
        return nextLink;
    }

    /**
     * @param nextLink Next data page link for fragmented rows.
     */
    public void setNextLink(long nextLink) {
        this.nextLink = nextLink;
    }
}
