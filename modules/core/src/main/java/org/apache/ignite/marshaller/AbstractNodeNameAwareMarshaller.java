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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Marshaller allowing for {@link Ignition#localIgnite()} calls.
 */
public abstract class AbstractNodeNameAwareMarshaller extends AbstractMarshaller {
    /** Whether node name is set. */
    private volatile boolean nodeNameSet;

    /** Node name. */
    private volatile String nodeName = U.LOC_IGNITE_NAME_EMPTY;

    /**
     * Set node name.
     *
     * @param nodeName Node name.
     */
    public void nodeName(@Nullable String nodeName) {
        if (!nodeNameSet) {
            this.nodeName = nodeName;

            nodeNameSet = true;
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(@Nullable Object obj) throws IgniteCheckedException {
        String oldNodeName = IgniteUtils.setCurrentIgniteName(nodeName);

        try {
            return marshal0(obj);
        }
        finally {
            IgniteUtils.restoreOldIgniteName(oldNodeName, nodeName);
        }
    }

    /** {@inheritDoc} */
    @Override public void marshal(@Nullable Object obj, OutputStream out) throws IgniteCheckedException {
        String oldNodeName = IgniteUtils.setCurrentIgniteName(nodeName);

        try {
            marshal0(obj, out);
        }
        finally {
            IgniteUtils.restoreOldIgniteName(oldNodeName, nodeName);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] arr, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        String oldNodeName = IgniteUtils.setCurrentIgniteName(nodeName);

        try {
            return unmarshal0(arr, clsLdr);
        }
        finally {
            IgniteUtils.restoreOldIgniteName(oldNodeName, nodeName);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(InputStream in, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        String oldNodeName = IgniteUtils.setCurrentIgniteName(nodeName);

        try {
            return unmarshal0(in, clsLdr);
        }
        finally {
            IgniteUtils.restoreOldIgniteName(oldNodeName, nodeName);
        }
    }

    /**
     * Marshals object to the output stream. This method should not close
     * given output stream.
     *
     * @param obj Object to marshal.
     * @param out Output stream to marshal into.
     * @throws IgniteCheckedException If marshalling failed.
     */
    protected abstract void marshal0(@Nullable Object obj, OutputStream out) throws IgniteCheckedException;

    /**
     * Marshals object to byte array.
     *
     * @param obj Object to marshal.
     * @return Byte array.
     * @throws IgniteCheckedException If marshalling failed.
     */
    protected abstract byte[] marshal0(@Nullable Object obj) throws IgniteCheckedException;

    /**
     * Unmarshals object from the input stream using given class loader.
     * This method should not close given input stream.
     *
     * @param <T> Type of unmarshalled object.
     * @param in Input stream.
     * @param clsLdr Class loader to use.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    protected abstract <T> T unmarshal0(InputStream in, @Nullable ClassLoader clsLdr) throws IgniteCheckedException;

    /**
     * Unmarshals object from byte array using given class loader.
     *
     * @param <T> Type of unmarshalled object.
     * @param arr Byte array.
     * @param clsLdr Class loader to use.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    protected abstract <T> T unmarshal0(byte[] arr, @Nullable ClassLoader clsLdr) throws IgniteCheckedException;
}
