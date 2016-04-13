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

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.resources.IgniteInstanceResource;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import java.math.BigDecimal;

/**
 * Test filter.
 */
@SuppressWarnings({"FieldCanBeLocal", "FloatingPointEquality"})
public class PlatformCacheEntryEventFilter implements CacheEntryEventSerializableFilter {
    /** Property to be set from platform. */
    private String startsWith = "-";

    /** Property to be set from platform. */
    private char charField;

    /** Property to be set from platform. */
    private byte byteField;

    /** Property to be set from platform. */
    private byte sbyteField;

    /** Property to be set from platform. */
    private short shortField;

    /** Property to be set from platform. */
    private short ushortField;

    /** Property to be set from platform. */
    private int intField;

    /** Property to be set from platform. */
    private int uintField;

    /** Property to be set from platform. */
    private long longField;

    /** Property to be set from platform. */
    private long ulongField;

    /** Property to be set from platform. */
    private float floatField;

    /** Property to be set from platform. */
    private double doubleField;

    /** Property to be set from platform. */
    private BigDecimal decimalField;

    /** Property to be set from platform. */
    private boolean boolField;

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
        assert sbyteField == 2;
        assert shortField == 3;
        assert ushortField == 4;
        assert intField == 5;
        assert uintField == 6;
        assert longField == 7;
        assert ulongField == 8;
        assert floatField == (float)9.99;
        assert doubleField == 10.123;
        assert "11.245".equals(decimalField.toString());
        assert boolField;

        return ((String)event.getValue()).startsWith(startsWith);
    }
}
