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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;

/**
 * Serializer for representing JO as byte array in inline.
 */
public class JavaObjectKeySerializer {
    /** Class loader. */
    private final ClassLoader clsLdr;

    /** Marshaller. */
    private final Marshaller marshaller;

    /**
     * Constructor.
     *
     * @param cfg Ignite configuration.
     */
    public JavaObjectKeySerializer(IgniteConfiguration cfg) {
        this(U.resolveClassLoader(cfg), cfg.getMarshaller());
    }

    /**
     * Constructor.
     *
     * @param marshaller Marshaller.
     * @param clsLdr Class loader.
     */
    public JavaObjectKeySerializer(ClassLoader clsLdr, Marshaller marshaller) {
        this.clsLdr = clsLdr;
        this.marshaller = marshaller;
    }

    /** */
    public byte[] serialize(Object obj) throws IgniteCheckedException {
        return U.marshal(marshaller, obj);
    }

    /** */
    public Object deserialize(byte[] bytes) throws IgniteCheckedException {
        return U.unmarshal(marshaller, bytes, clsLdr);
    }
}
