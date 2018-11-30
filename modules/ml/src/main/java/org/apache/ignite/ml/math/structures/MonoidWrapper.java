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

package org.apache.ignite.ml.math.structures;

import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;

public class MonoidWrapper<T> implements Monoid<MonoidWrapper<T>> {
    private T val;
    private final T mempty;
    private final IgniteBinaryOperator<T> mappend;

    MonoidWrapper(T val, IgniteBinaryOperator<T> mappend, T mempty) {
        this.val = val;
        this.mempty = mempty;
        this.mappend = mappend;
    }

    @Override public MonoidWrapper<T> mappend(MonoidWrapper<T> other) {
        val = mappend.apply(val, other.val);

        return this;
    }

    @Override public MonoidWrapper<T> mempty() {
        return new MonoidWrapper<>(mempty, mappend, mempty);
    }

    public T val() {
        return val;
    }
}
