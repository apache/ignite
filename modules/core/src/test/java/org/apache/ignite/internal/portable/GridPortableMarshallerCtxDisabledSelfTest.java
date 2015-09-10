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

package org.apache.ignite.internal.portable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallerContextAdapter;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableMarshalAware;
import org.apache.ignite.portable.PortableMetadata;
import org.apache.ignite.portable.PortableReader;
import org.apache.ignite.portable.PortableWriter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridPortableMarshallerCtxDisabledSelfTest extends GridCommonAbstractTest {
    /** */
    protected static final PortableMetaDataHandler META_HND = new PortableMetaDataHandler() {
        @Override public void addMeta(int typeId, PortableMetadata meta) {
            // No-op.
        }

        @Override public PortableMetadata metadata(int typeId) {
            return null;
        }
    };

    /**
     * @throws Exception If failed.
     */
    public void testObjectExchange() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();
        marsh.setContext(new MarshallerContextWithNoStorage());

        PortableContext context = new PortableContext(META_HND, null);

        IgniteUtils.invoke(PortableMarshaller.class, marsh, "setPortableContext", context);

        SimpleObject simpleObj = new SimpleObject();

        simpleObj.b = 2;
        simpleObj.bArr = new byte[] {2, 3, 4, 5, 5};
        simpleObj.c = 'A';
        simpleObj.enumVal = TestEnum.D;
        simpleObj.objArr = new Object[] {"hello", "world", "from", "me"};
        simpleObj.enumArr = new TestEnum[] {TestEnum.C, TestEnum.B};

        SimpleObject otherObj = new SimpleObject();

        otherObj.b = 3;
        otherObj.bArr = new byte[] {5, 3, 4};

        simpleObj.otherObj = otherObj;

        assertEquals(simpleObj, marsh.unmarshal(marsh.marshal(simpleObj), null));

        SimplePortable simplePortable = new SimplePortable();

        simplePortable.str = "portable";
        simplePortable.arr = new long[] {100, 200, 300};

        assertEquals(simplePortable, marsh.unmarshal(marsh.marshal(simplePortable), null));

        SimpleExternalizable simpleExtr = new SimpleExternalizable();

        simpleExtr.str = "externalizable";
        simpleExtr.arr = new long[] {20000, 300000, 400000};

        assertEquals(simpleExtr, marsh.unmarshal(marsh.marshal(simpleExtr), null));
    }

    /**
     * Marshaller context with no storage. Platform has to work in such environment as well by marshalling class name of
     * a portable object.
     */
    private static class MarshallerContextWithNoStorage extends MarshallerContextAdapter {
        /** */
        public MarshallerContextWithNoStorage() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override protected boolean registerClassName(int id, String clsName) throws IgniteCheckedException {
            return false;
        }

        /** {@inheritDoc} */
        @Override protected String className(int id) throws IgniteCheckedException {
            return null;
        }
    }

    /**
     */
    private enum TestEnum {
        A, B, C, D, E
    }

    /**
     */
    private static class SimpleObject {
        /** */
        private byte b;

        /** */
        private char c;

        /** */
        private byte[] bArr;

        /** */
        private Object[] objArr;

        /** */
        private TestEnum enumVal;

        /** */
        private TestEnum[] enumArr;

        private SimpleObject otherObj;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SimpleObject object = (SimpleObject)o;

            if (b != object.b)
                return false;

            if (c != object.c)
                return false;

            if (!Arrays.equals(bArr, object.bArr))
                return false;

            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            if (!Arrays.equals(objArr, object.objArr))
                return false;

            if (enumVal != object.enumVal)
                return false;

            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            if (!Arrays.equals(enumArr, object.enumArr))
                return false;

            return !(otherObj != null ? !otherObj.equals(object.otherObj) : object.otherObj != null);
        }
    }

    /**
     *
     */
    private static class SimplePortable implements PortableMarshalAware {
        /** */
        private String str;

        /** */
        private long[] arr;

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            writer.writeString("str", str);
            writer.writeLongArray("longArr", arr);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            str = reader.readString("str");
            arr = reader.readLongArray("longArr");
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SimplePortable that = (SimplePortable)o;

            if (str != null ? !str.equals(that.str) : that.str != null)
                return false;

            return Arrays.equals(arr, that.arr);
        }
    }

    /**
     *
     */
    private static class SimpleExternalizable implements Externalizable {
        /** */
        private String str;

        /** */
        private long[] arr;

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(str);
            out.writeObject(arr);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            str = in.readUTF();
            arr = (long[])in.readObject();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SimpleExternalizable that = (SimpleExternalizable)o;

            if (str != null ? !str.equals(that.str) : that.str != null)
                return false;

            return Arrays.equals(arr, that.arr);
        }
    }
}