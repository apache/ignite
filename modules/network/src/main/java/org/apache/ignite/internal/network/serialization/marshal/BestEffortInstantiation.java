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

package org.apache.ignite.internal.network.serialization.marshal;

import java.util.List;

/**
 * Instantiation strategy that delegates to a list of instantiation strategies and uses the first one that
 * announces support for instantiating a given class.
 */
class BestEffortInstantiation implements Instantiation {
    private final List<Instantiation> delegates;

    BestEffortInstantiation(Instantiation... delegates) {
        this.delegates = List.of(delegates);
    }

    /** {@inheritDoc} */
    @Override
    public boolean supports(Class<?> objectClass) {
        for (Instantiation delegate : delegates) {
            if (delegate.supports(objectClass)) {
                return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public Object newInstance(Class<?> objectClass) throws InstantiationException {
        for (Instantiation delegate : delegates) {
            if (delegate.supports(objectClass)) {
                return delegate.newInstance(objectClass);
            }
        }

        throw new InstantiationException("No delegate supports " + objectClass);
    }
}
