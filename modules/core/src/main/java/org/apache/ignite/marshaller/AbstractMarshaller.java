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

package org.apache.ignite.marshaller;

import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.io.GridByteArrayInputStream;
import org.apache.ignite.internal.util.io.GridByteArrayOutputStream;

/**
 * Base class for marshallers. Provides default implementations of methods
 * that work with byte array or {@link GridByteArrayList}. These implementations
 * use {@link GridByteArrayInputStream} or {@link GridByteArrayOutputStream}
 * to marshal and unmarshal objects.
 */
public abstract class AbstractMarshaller implements Marshaller {
    /** Default initial buffer size for the {@link GridByteArrayOutputStream}. */
    public static final int DFLT_BUFFER_SIZE = 512;

    /** Context. */
    protected MarshallerContext ctx;

    /**
     * Undeployment callback invoked when class loader is being undeployed.
     *
     * Some marshallers may want to clean their internal state that uses the undeployed class loader somehow.
     *
     * @param ldr Class loader being undeployed.
     */
    public abstract void onUndeploy(ClassLoader ldr);

    /**
     * @return Marshaller context.
     */
    public MarshallerContext getContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public void setContext(MarshallerContext ctx) {
        this.ctx = ctx;
    }
}
