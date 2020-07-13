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

package org.apache.ignite.internal.processors.platform;

import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Default Java object factory implementation.
 */
public class PlatformDefaultJavaObjectFactory<T> implements PlatformJavaObjectFactoryEx<T> {
    /** Class name. */
    private String clsName;

    /** Properties. */
    private Map<String, Object> props;

    /** {@inheritDoc} */
    @Override public void initialize(@Nullable Object payload, @Nullable Map<String, Object> props) {
        if (payload == null)
            throw new IgniteException("Java object class name is not provided.");

        assert payload instanceof String;

        clsName = (String)payload;

        this.props = props;
    }

    /** {@inheritDoc} */
    @Override public T create() {
        T res = PlatformUtils.createJavaObject(clsName);

        PlatformUtils.initializeJavaObject(res, clsName, props, null);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformDefaultJavaObjectFactory.class, this);
    }
}
