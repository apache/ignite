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

package org.apache.ignite.internal.network.serialization.marshal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactoryContext;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DefaultUserObjectMarshaller}.
 */
class DefaultUserObjectMarshallerTest {
    private final ClassDescriptorFactoryContext descriptorRegistry = new ClassDescriptorFactoryContext();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    private static final int WRITE_REPLACE_INCREMENT = 1_000_000;
    private static final int READ_RESOLVE_INCREMENT = 1_000;

    @Test
    void usesExactlyOneDescriptorWhenMarshallingExternalizable() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SimpleExternalizable(42));

        ClassDescriptor expectedDescriptor = descriptorRegistry.getDescriptor(SimpleExternalizable.class);
        assertThat(expectedDescriptor, is(notNullValue()));
        assertThat(marshalled.usedDescriptors(), is(equalTo(List.of(expectedDescriptor))));
    }

    @Test
    void marshalsAndUnmarshalsExternalizable() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SimpleExternalizable(42));

        SimpleExternalizable unmarshalled = marshaller.unmarshal(marshalled.bytes());

        assertThat(unmarshalled, is(notNullValue()));
        assertThat(unmarshalled.intValue, is(42));
    }

    @Test
    void appliesWriteReplaceOnExternalizable() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new ExternalizableWithWriteReplace(42));

        SimpleExternalizable unmarshalled = marshaller.unmarshal(marshalled.bytes());

        assertThat(unmarshalled.intValue, is(equalTo(42 + WRITE_REPLACE_INCREMENT)));
    }

    @Test
    void appliesReadResolveOnExternalizable() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new ExternalizableWithReadResolve(42));

        SimpleExternalizable unmarshalled = marshaller.unmarshal(marshalled.bytes());

        assertThat(unmarshalled.intValue, is(equalTo(42 + READ_RESOLVE_INCREMENT)));
    }

    @Test
    void appliesBothWriteReplaceAndReadResolveOnExternalizable() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new ExternalizableWithWriteReplaceReadResolve(42));

        SimpleExternalizable unmarshalled = marshaller.unmarshal(marshalled.bytes());

        assertThat(unmarshalled.intValue, is(equalTo(42 + WRITE_REPLACE_INCREMENT + READ_RESOLVE_INCREMENT)));
    }

    /**
     * An {@link Externalizable} that does not have {@code writeReplace()}/{@code readResolve()} methods.
     */
    private static class SimpleExternalizable implements Externalizable {
        int intValue;

        public SimpleExternalizable() {
        }

        public SimpleExternalizable(int intValue) {
            this.intValue = intValue;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(-intValue);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException {
            intValue = -in.readInt();
        }
    }

    private static class ExternalizableWithWriteReplace extends SimpleExternalizable {
        public ExternalizableWithWriteReplace() {
        }

        public ExternalizableWithWriteReplace(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new ExternalizableWithWriteReplaceReadResolve(intValue + 1_000_000);
        }
    }

    private static class ExternalizableWithReadResolve extends SimpleExternalizable {
        public ExternalizableWithReadResolve() {
        }

        public ExternalizableWithReadResolve(int intValue) {
            super(intValue);
        }

        private Object readResolve() {
            return new ExternalizableWithWriteReplaceReadResolve(intValue + 1_000);
        }
    }

    private static class ExternalizableWithWriteReplaceReadResolve extends SimpleExternalizable {
        public ExternalizableWithWriteReplaceReadResolve() {
        }

        public ExternalizableWithWriteReplaceReadResolve(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new ExternalizableWithWriteReplaceReadResolve(intValue + 1_000_000);
        }

        private Object readResolve() {
            return new ExternalizableWithWriteReplaceReadResolve(intValue + 1_000);
        }
    }
}
