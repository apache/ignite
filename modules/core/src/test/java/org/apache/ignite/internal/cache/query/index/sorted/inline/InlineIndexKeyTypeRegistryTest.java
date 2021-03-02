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

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;
import org.apache.ignite.internal.cache.query.index.sorted.keys.DateIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IntegerIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NullIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.PlainJavaObjectIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.StringIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.TimestampIndexKey;
import org.junit.Test;

/** */
public class InlineIndexKeyTypeRegistryTest {
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
        assert t == IndexKeyTypes.INT;

        t = InlineIndexKeyTypeRegistry.get(NullIndexKey.INSTANCE, IndexKeyTypes.JAVA_OBJECT, pojoArrayKeyTypeSettings).type();
        assert t == IndexKeyTypes.JAVA_OBJECT;

        t = InlineIndexKeyTypeRegistry.get(NullIndexKey.INSTANCE, IndexKeyTypes.JAVA_OBJECT, pojoHashKeyTypeSettings).type();
        assert t == IndexKeyTypes.JAVA_OBJECT;
    }

    /** */
    @Test
    public void testObjectCheck() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(new IntegerIndexKey(3), IndexKeyTypes.JAVA_OBJECT, pojoArrayKeyTypeSettings);
        assert t.type() == IndexKeyTypes.INT;

        t = InlineIndexKeyTypeRegistry.get(new PlainJavaObjectIndexKey(new BinaryObjectImpl(), null), IndexKeyTypes.JAVA_OBJECT, pojoArrayKeyTypeSettings);
        assert t.type() == IndexKeyTypes.JAVA_OBJECT;

        t = InlineIndexKeyTypeRegistry.get(new PlainJavaObjectIndexKey(new BinaryObjectImpl(), null), IndexKeyTypes.INT, pojoArrayKeyTypeSettings);
        assert t.type() == IndexKeyTypes.JAVA_OBJECT;

        t = InlineIndexKeyTypeRegistry.get(new IntegerIndexKey(3), IndexKeyTypes.JAVA_OBJECT, pojoHashKeyTypeSettings);
        assert t.type() == IndexKeyTypes.INT;
    }

    /** */
    @Test
    public void testMismatchType() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(new IntegerIndexKey(3), 11, pojoArrayKeyTypeSettings);
        assert t.type() == IndexKeyTypes.INT;
    }

    /** */
    @Test
    public void testDateTypes() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(new TimestampIndexKey(new java.util.Date(0L)), IndexKeyTypes.TIMESTAMP, pojoArrayKeyTypeSettings);
        assert t.type() == IndexKeyTypes.TIMESTAMP;

        t = InlineIndexKeyTypeRegistry.get(new TimestampIndexKey(LocalDateTime.now()), IndexKeyTypes.TIMESTAMP, pojoArrayKeyTypeSettings);
        assert t.type() == IndexKeyTypes.TIMESTAMP;

        t = InlineIndexKeyTypeRegistry.get(new TimestampIndexKey(new Timestamp(0L)), IndexKeyTypes.TIMESTAMP, pojoArrayKeyTypeSettings);
        assert t.type() == IndexKeyTypes.TIMESTAMP;

        t = InlineIndexKeyTypeRegistry.get(new DateIndexKey(new java.sql.Date(0L)), IndexKeyTypes.TIMESTAMP, pojoArrayKeyTypeSettings);
        assert t.type() == IndexKeyTypes.DATE;

        t = InlineIndexKeyTypeRegistry.get(new DateIndexKey(LocalDate.now()), IndexKeyTypes.TIMESTAMP, pojoArrayKeyTypeSettings);
        assert t.type() == IndexKeyTypes.DATE;
    }

    /** */
    @Test
    public void testStrings() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(new StringIndexKey(""), IndexKeyTypes.STRING, pojoArrayKeyTypeSettings);
        assert t.type() == IndexKeyTypes.STRING;

        t = InlineIndexKeyTypeRegistry.get(new StringIndexKey(""), IndexKeyTypes.STRING, strNoCompareKeyTypeSettings);
        assert t.type() == IndexKeyTypes.STRING;
    }
}
