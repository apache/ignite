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

package org.apache.ignite.internal.cache.query.index.sorted.inline;

import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IntegerIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NullIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.PlainJavaObjectIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.StringIndexKey;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class InlineIndexKeyTypeRegistryTest extends GridCommonAbstractTest {
    /** */
    private static final IndexKeyTypeSettings pojoHashKeyTypeSettings =
        new IndexKeyTypeSettings();

    /** */
    private static final IndexKeyTypeSettings pojoArrayKeyTypeSettings =
        new IndexKeyTypeSettings().inlineObjHash(false);

    /** */
    private static final IndexKeyTypeSettings strNoCompareKeyTypeSettings =
        new IndexKeyTypeSettings().inlineObjHash(false).stringOptimizedCompare(false);

    /** */
    @Test
    public void testNulls() {
        int t = InlineIndexKeyTypeRegistry.get(NullIndexKey.INSTANCE, IndexKeyTypes.INT, pojoArrayKeyTypeSettings).type();
        assertEquals(IndexKeyTypes.INT, t);

        t = InlineIndexKeyTypeRegistry.get(NullIndexKey.INSTANCE, IndexKeyTypes.JAVA_OBJECT, pojoArrayKeyTypeSettings).type();
        assertEquals(IndexKeyTypes.JAVA_OBJECT, t);

        t = InlineIndexKeyTypeRegistry.get(NullIndexKey.INSTANCE, IndexKeyTypes.JAVA_OBJECT, pojoHashKeyTypeSettings).type();
        assertEquals(IndexKeyTypes.JAVA_OBJECT, t);
    }

    /** */
    @Test
    public void testObjectCheck() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(new IntegerIndexKey(3), IndexKeyTypes.JAVA_OBJECT, pojoArrayKeyTypeSettings);
        assertEquals(IndexKeyTypes.INT, t.type());

        t = InlineIndexKeyTypeRegistry.get(
            new PlainJavaObjectIndexKey(new BinaryObjectImpl(), null),
            IndexKeyTypes.JAVA_OBJECT,
            pojoArrayKeyTypeSettings
        );
        assertEquals(IndexKeyTypes.JAVA_OBJECT, t.type());

        t = InlineIndexKeyTypeRegistry.get(
            new PlainJavaObjectIndexKey(new BinaryObjectImpl(), null),
            IndexKeyTypes.INT,
            pojoArrayKeyTypeSettings
        );
        assertEquals(IndexKeyTypes.JAVA_OBJECT, t.type());

        t = InlineIndexKeyTypeRegistry.get(new IntegerIndexKey(3), IndexKeyTypes.JAVA_OBJECT, pojoHashKeyTypeSettings);
        assertEquals(IndexKeyTypes.INT, t.type());
    }

    /** */
    @Test
    public void testMismatchType() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(new IntegerIndexKey(3), 11, pojoArrayKeyTypeSettings);
        assertEquals(IndexKeyTypes.INT, t.type());
    }

    /** */
    @Test
    public void testStrings() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(new StringIndexKey(""), IndexKeyTypes.STRING, pojoArrayKeyTypeSettings);
        assertEquals(IndexKeyTypes.STRING, t.type());

        t = InlineIndexKeyTypeRegistry.get(new StringIndexKey(""), IndexKeyTypes.STRING, strNoCompareKeyTypeSettings);
        assertEquals(IndexKeyTypes.STRING, t.type());
    }
}
