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

package org.apache.ignite.internal.cdc;

import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.platform.PlatformType;

/** */
public class TypeMappingImpl implements TypeMapping {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private final int typeId;

    /** */
    private final String typeName;

    /** */
    private final PlatformType platform;

    /**
     * @param typeId Type id.
     * @param typeName Type name.
     * @param platform Platform.
     */
    public TypeMappingImpl(int typeId, String typeName, PlatformType platform) {
        this.typeId = typeId;
        this.typeName = typeName;
        this.platform = platform;
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return typeId;
    }

    /** {@inheritDoc} */
    @Override public String typeName() {
        return typeName;
    }

    /** {@inheritDoc} */
    @Override public PlatformType platformType() {
        return platform;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TypeMappingImpl.class, this);
    }
}
