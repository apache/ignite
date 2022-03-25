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

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.util.typedef.F;

/** */
public final class QueryContext {
    /** */
    private static final Object[] EMPTY = {};
    
    /** */
    private final Object[] params;

    /** */
    private QueryContext(Object[] params) {
        this.params = params;
    }

    /**
     * Finds an instance of an interface implemented by this object,
     * or returns null if this object does not support that interface.
     */
    public <C> C unwrap(Class<C> aClass) {
        if (Object[].class == aClass)
            return aClass.cast(params);

        return Arrays.stream(params).filter(aClass::isInstance).findFirst().map(aClass::cast).orElse(null);
    }

    /**
     * @param params Context parameters.
     * @return Query context.
     */
    public static QueryContext of(Object... params) {
        return !F.isEmpty(params) ? new QueryContext(build(null, params).toArray()) : new QueryContext(EMPTY);
    }

    /** */
    private static List<Object> build(List<Object> dst, Object[] src) {
        if (dst == null)
            dst = new ArrayList<>();

        for (Object obj : src) {
            if (obj == null)
                continue;

            if (obj.getClass() == QueryContext.class)
                build(dst, ((QueryContext)obj).params);
            else
                dst.add(obj);
        }

        return dst;
    }
}
