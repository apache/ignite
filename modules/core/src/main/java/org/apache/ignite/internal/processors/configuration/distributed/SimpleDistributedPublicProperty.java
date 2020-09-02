/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.configuration.distributed;

import java.io.Serializable;
import org.apache.ignite.plugin.security.SecurityPermission;

/**
 * Public wrapper for SimpleDistributedProperty.
 * Use this class to provide access to the property management by command line interface.
 */
public class SimpleDistributedPublicProperty<T extends Serializable>
    extends SimpleDistributedProperty<T> implements PublicProperty<T> {
    /** Public delegate. */
    private final PublicProperty<T> publicDelegate;

    /**
     * @param name Name of property.
     * @param publicDelegate Delegate ofthe implementation the PublicProperty interface.
     */
    public SimpleDistributedPublicProperty(String name, PublicProperty<T> publicDelegate) {
        super(name);
        this.publicDelegate = publicDelegate;
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return publicDelegate.description();
    }

    /** {@inheritDoc} */
    @Override public T parse(String str) {
        return publicDelegate.parse(str);
    }

    /** {@inheritDoc} */
    @Override public String format(T val) {
        return publicDelegate.format(val);
    }

    /** {@inheritDoc} */
    @Override public SecurityPermission readPermission() {
        return publicDelegate.readPermission();
    }

    /** {@inheritDoc} */
    @Override public SecurityPermission writePermission() {
        return publicDelegate.writePermission();
    }
}
