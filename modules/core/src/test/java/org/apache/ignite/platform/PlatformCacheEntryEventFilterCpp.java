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

package org.apache.ignite.platform;

import java.util.UUID;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Test filter.
 */
@SuppressWarnings({"FieldCanBeLocal", "FloatingPointEquality", "MismatchedReadAndWriteOfArray", "unused"
})
public class PlatformCacheEntryEventFilterCpp implements CacheEntryEventSerializableFilter {
    /** Property to be set from platform. */
    private String startsWith = "-";

    /** Property to be set from platform. */
    private char charField;

    /** Property to be set from platform. */
    private byte byteField;

    /** Property to be set from platform. */
    private short shortField;

    /** Property to be set from platform. */
    private int intField;

    /** Property to be set from platform. */
    private long longField;

    /** Property to be set from platform. */
    private float floatField;

    /** Property to be set from platform. */
    private double doubleField;

    /** Property to be set from platform. */
    private boolean boolField;

    /** Property to be set from platform. */
    private UUID guidField;

    /** Property to be set from platform. */
    private BinaryObject objField;

    /** Property to be set from platform. */
    private char[] charArr;

    /** Property to be set from platform. */
    private byte[] byteArr;

    /** Property to be set from platform. */
    private short[] shortArr;

    /** Property to be set from platform. */
    private int[] intArr;

    /** Property to be set from platform. */
    private long[] longArr;

    /** Injected instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public boolean evaluate(CacheEntryEvent event) throws CacheEntryListenerException {
        // check injection
        assert ignite != null;

        // check fields
        assert charField == 'a';
        assert byteField == 1;
        assert shortField == 3;
        assert intField == 5;
        assert longField == 7;
        assert floatField == (float)9.99;
        assert doubleField == 10.123;
        assert boolField;
        assert guidField.equals(UUID.fromString("1c579241-509d-47c6-a1a0-87462ae31e59"));

        // check arrays
        assert charArr[0] == 'a';
        assert byteArr[0] == 1;
        assert shortArr[0] == 3;
        assert intArr[0] == 5;
        assert longArr[0] == 7;
        // check binary object
        assert objField != null;
        assert Integer.valueOf(1).equals(objField.field("i32Field"));
        assert "2".equals(objField.field("strField"));

        Object value = event.getValue();

        if (value instanceof String)
            return ((String)value).startsWith(startsWith);

        assert value instanceof BinaryObject;

        return ((String)((BinaryObject)value).field("String")).startsWith(startsWith);
    }
}
