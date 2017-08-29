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

package org.apache.ignite.cache.store.jdbc;

import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.binary.BinaryStringEncoding.ENC_NAME_WINDOWS_1251;

/**
 * Test for {@link CacheJdbcPojoStore} with binary marshaller. Marshaller's string encoding is windows-1251.
 */
public class CacheJdbcPojoStoreBinaryMarshallerWin1251SelfTest extends CacheJdbcPojoStoreBinaryMarshallerAbstractSelfTest {
    /** {@inheritDoc} */
    @Override @Nullable protected String stringEncoding() {
        return ENC_NAME_WINDOWS_1251;
    }
}
