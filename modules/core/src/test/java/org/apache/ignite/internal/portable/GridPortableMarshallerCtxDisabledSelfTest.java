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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallerContextAdapter;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableMetadata;
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

        SimpleObject obj0 = new SimpleObject();

        obj0.b = 2;
        obj0.bArr = new byte[] {2, 3, 4, 5, 5};
        obj0.c = 'A';
        obj0.enumVal = TestEnum.D;
        obj0.objArr = new Object[] {"hello", "world", "from", "me"};
        obj0.enumArr = new TestEnum[] {TestEnum.C, TestEnum.B};

        byte[] arr = marsh.marshal(obj0);

        SimpleObject obj2 = marsh.unmarshal(arr, null);

        assertEquals(obj0.b, obj2.b);
        assertEquals(obj0.c, obj2.c);
        assertEquals(obj0.enumVal, obj2.enumVal);

        for (int i = 0; i < obj0.bArr.length; i++)
            assertEquals(obj0.bArr[i], obj2.bArr[i]);

        for (int i = 0; i < obj0.objArr.length; i++)
            assertEquals(obj0.objArr[i], obj2.objArr[i]);

        for (int i = 0; i < obj0.enumArr.length; i++)
            assertEquals(obj0.enumArr[i], obj2.enumArr[i]);
    }

    /**
     * Marshaller context with no storage. Platform has to work in such environment as well by marshalling class name
     * of a portable object.
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
    }
}