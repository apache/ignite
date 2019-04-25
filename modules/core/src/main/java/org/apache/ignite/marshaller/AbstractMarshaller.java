/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
