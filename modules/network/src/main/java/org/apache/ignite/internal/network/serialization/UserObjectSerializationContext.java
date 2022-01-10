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

package org.apache.ignite.internal.network.serialization;

import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;

/** User object serialization objects wrapper. */
public class UserObjectSerializationContext {
    private final ClassDescriptorRegistry descriptorRegistry;
    private final ClassDescriptorFactory descriptorFactory;
    private final UserObjectMarshaller marshaller;

    /**
     * Constructor.
     *
     * @param descriptorRegistry Descriptor registry.
     * @param descriptorFactory Descriptor factory.
     * @param marshaller User object marshaller.
     */
    public UserObjectSerializationContext(ClassDescriptorRegistry descriptorRegistry,
            ClassDescriptorFactory descriptorFactory, UserObjectMarshaller marshaller) {
        this.descriptorRegistry = descriptorRegistry;
        this.descriptorFactory = descriptorFactory;
        this.marshaller = marshaller;
    }

    public ClassDescriptorRegistry descriptorRegistry() {
        return descriptorRegistry;
    }

    public ClassDescriptorFactory descriptorFactory() {
        return descriptorFactory;
    }

    public UserObjectMarshaller marshaller() {
        return marshaller;
    }
}
