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
package org.apache.ignite.internal.processors.cacheobject;

import java.util.Collection;
import java.util.Map;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class NoOpBinary implements IgniteBinary {
    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public <T> T toBinary(@Nullable Object obj) throws BinaryObjectException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(String typeName) throws BinaryObjectException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(BinaryObject binaryObj) throws BinaryObjectException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public BinaryType type(Class<?> cls) throws BinaryObjectException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public BinaryType type(String typeName) throws BinaryObjectException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public BinaryType type(int typeId) throws BinaryObjectException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryType> types() throws BinaryObjectException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public BinaryObject buildEnum(String typeName, int ord) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public BinaryObject buildEnum(String typeName, String name) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public BinaryType registerEnum(String typeName, Map<String, Integer> vals) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    private BinaryObjectException unsupported() {
        return new BinaryObjectException("Binary marshaller is not configured.");
    }
}
