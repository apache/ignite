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
import org.apache.ignite.internal.cache.query.index.sorted.NullKey;
import org.junit.Test;

/** */
public class InlineIndexKeyTypeRegistryTest {
    /** */
    @Test
    public void testNulls() {
        int t = InlineIndexKeyTypeRegistry.get(NullKey.class, IndexKeyTypes.INT, false).type();
        assert t == IndexKeyTypes.INT;

        t = InlineIndexKeyTypeRegistry.get(NullKey.class, IndexKeyTypes.JAVA_OBJECT, false).type();
        assert t == IndexKeyTypes.JAVA_OBJECT;

        t = InlineIndexKeyTypeRegistry.get(NullKey.class, IndexKeyTypes.JAVA_OBJECT, true).type();
        assert t == IndexKeyTypes.JAVA_OBJECT;
    }

    /** */
    @Test
    public void testObjectCheck() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(Integer.class, IndexKeyTypes.JAVA_OBJECT, false);
        assert t.type() == IndexKeyTypes.INT;

        t = InlineIndexKeyTypeRegistry.get(BinaryObjectImpl.class, IndexKeyTypes.JAVA_OBJECT, false);
        assert t.type() == IndexKeyTypes.JAVA_OBJECT;

        t = InlineIndexKeyTypeRegistry.get(BinaryObjectImpl.class, IndexKeyTypes.INT, false);
        assert t.type() == IndexKeyTypes.JAVA_OBJECT;;

        t = InlineIndexKeyTypeRegistry.get(Integer.class, IndexKeyTypes.JAVA_OBJECT, true);
        assert t.type() == IndexKeyTypes.INT;
    }

    /** */
    @Test
    public void testNonRegistredClass() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(InlineIndexKeyTypeRegistryTest.class, IndexKeyTypes.INT, false);
        assert t == null;
    }

    /** */
    @Test
    public void testMismatchType() {
        InlineIndexKeyType t = InlineIndexKeyTypeRegistry.get(Integer.class, 11, false);
        assert t.type() == IndexKeyTypes.INT;
    }
}
