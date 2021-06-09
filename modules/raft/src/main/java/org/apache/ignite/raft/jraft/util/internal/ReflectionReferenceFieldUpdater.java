/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util.internal;

import java.lang.reflect.Field;

/**
 *
 */
final class ReflectionReferenceFieldUpdater<U, W> implements ReferenceFieldUpdater<U, W> {
    private final Field field;

    ReflectionReferenceFieldUpdater(Class<? super U> tClass, String fieldName) throws NoSuchFieldException {
        this.field = tClass.getDeclaredField(fieldName);
        this.field.setAccessible(true);
    }

    @Override
    public void set(final U obj, final W newValue) {
        try {
            this.field.set(obj, newValue);
        }
        catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public W get(final U obj) {
        try {
            return (W) this.field.get(obj);
        }
        catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
