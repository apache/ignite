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

package org.apache.ignite.cache.hibernate.config;

import java.util.Objects;
import org.apache.ignite.binary.BinaryAbstractIdentityResolver;
import org.apache.ignite.binary.BinaryIdentityResolver;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryTypeConfiguration;

/**
 * This configuration provides correct {@link BinaryIdentityResolver} implementation
 * for Hibernate CacheKey class can be used as a key object.
 *
 * Note: for Hibernate version < 5.0 {@link HibernateCacheKeyTypeConfiguration} should be used.

 */
public class Hibernate5CacheKeyTypeConfiguration extends BinaryTypeConfiguration {

    /** {@inheritDoc} */
    public Hibernate5CacheKeyTypeConfiguration() {
        super("org.hibernate.cache.internal.CacheKeyImplementation");

        setIdentityResolver(new BinaryAbstractIdentityResolver() {
            @Override protected int hashCode0(BinaryObject obj) {
                return obj.field("id").hashCode();
            }

            @Override protected boolean equals0(BinaryObject o1, BinaryObject o2) {
                Object obj0 = o1.field("id");
                Object obj1 = o2.field("id");

                return Objects.equals(obj0, obj1);
            }
        });
    }
}
