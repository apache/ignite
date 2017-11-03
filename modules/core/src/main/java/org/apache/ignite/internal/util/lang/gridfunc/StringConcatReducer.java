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

package org.apache.ignite.internal.util.lang.gridfunc;

import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.lang.IgniteReducer;

/**
 * Reducer that concatenates strings using provided delimiter.
 */
public class StringConcatReducer implements IgniteReducer<String, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final String delim;

    /** */
    private SB sb;

    /** */
    private boolean first;

    /** */
    private final Object lock;

    /**
     * @param delim Delimiter (optional).
     */
    public StringConcatReducer(String delim) {
        this.delim = delim;
        sb = new SB();
        first = true;
        lock = new Object();
    }

    /** {@inheritDoc} */
    @Override public boolean collect(String s) {
        synchronized (lock) {
            if (!first && !GridFunc.isEmpty(delim))
                sb.a(delim);

            sb.a(s);

            first = false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String reduce() {
        synchronized (lock) {
            return sb.toString();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StringConcatReducer.class, this);
    }
}
