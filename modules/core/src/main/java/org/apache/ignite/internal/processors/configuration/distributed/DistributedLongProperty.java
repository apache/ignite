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

package org.apache.ignite.internal.processors.configuration.distributed;

/**
 * Implementation of {@link DistributedProperty} for {@link Long}.
 */
public class DistributedLongProperty extends DistributedComparableProperty<Long> {
    /** {@inheritDoc} */
    DistributedLongProperty(String name, String desc) {
        super(name, Long::parseLong, desc);
    }

    /**
     * @param name Name of property.
     * @param desc Description of property.
     * @return Property detached from processor.(Distributed updating are not accessable).
     */
    public static DistributedLongProperty detachedLongProperty(String name, String desc) {
        return new DistributedLongProperty(name, desc);
    }
}
