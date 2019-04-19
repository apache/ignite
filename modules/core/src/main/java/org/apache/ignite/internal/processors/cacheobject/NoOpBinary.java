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
