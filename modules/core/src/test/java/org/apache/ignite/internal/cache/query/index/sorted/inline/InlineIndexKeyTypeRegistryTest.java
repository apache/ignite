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
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
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
        IndexKeyType t = InlineIndexKeyTypeRegistry.get(NullIndexKey.INSTANCE, IndexKeyType.INT, pojoArrayKeyTypeSettings).type();
        assertEquals(IndexKeyType.INT, t);

        t = InlineIndexKeyTypeRegistry.get(NullIndexKey.INSTANCE, IndexKeyType.JAVA_OBJECT, pojoArrayKeyTypeSettings).type();
        assertEquals(IndexKeyType.JAVA_OBJECT, t);

        t = InlineIndexKeyTypeRegistry.get(NullIndexKey.INSTANCE, IndexKeyType.JAVA_OBJECT, pojoHashKeyTypeSettings).type();
        assertEquals(IndexKeyType.JAVA_OBJECT, t);
    }

    /** */
    @Test
    public void testObjectCheck() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(new IntegerIndexKey(3), IndexKeyType.JAVA_OBJECT, pojoArrayKeyTypeSettings);
        assertEquals(IndexKeyType.INT, t.type());

        t = InlineIndexKeyTypeRegistry.get(
            new PlainJavaObjectIndexKey(new BinaryObjectImpl(), null),
            IndexKeyType.JAVA_OBJECT,
            pojoArrayKeyTypeSettings
        );
        assertEquals(IndexKeyType.JAVA_OBJECT, t.type());

        t = InlineIndexKeyTypeRegistry.get(
            new PlainJavaObjectIndexKey(new BinaryObjectImpl(), null),
            IndexKeyType.INT,
            pojoArrayKeyTypeSettings
        );
        assertEquals(IndexKeyType.JAVA_OBJECT, t.type());

        t = InlineIndexKeyTypeRegistry.get(new IntegerIndexKey(3), IndexKeyType.JAVA_OBJECT, pojoHashKeyTypeSettings);
        assertEquals(IndexKeyType.INT, t.type());
    }

    /** */
    @Test
    public void testMismatchType() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(new IntegerIndexKey(3), IndexKeyType.TIMESTAMP, pojoArrayKeyTypeSettings);
        assertEquals(IndexKeyType.INT, t.type());
    }

    /** */
    @Test
    public void testStrings() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(new StringIndexKey(""), IndexKeyType.STRING, pojoArrayKeyTypeSettings);
        assertEquals(IndexKeyType.STRING, t.type());

        t = InlineIndexKeyTypeRegistry.get(new StringIndexKey(""), IndexKeyType.STRING, strNoCompareKeyTypeSettings);
        assertEquals(IndexKeyType.STRING, t.type());
    }
}
