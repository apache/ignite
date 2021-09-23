/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.configuration;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.configuration.DirectConfigurationProperty;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.DirectAccess;

/**
 * {@link DynamicProperty} extension that implements {@link DirectConfigurationProperty}.
 *
 * @see DirectAccess
 */
public class DirectDynamicProperty<T extends Serializable>
    extends DynamicProperty<T>
    implements DirectConfigurationProperty<T> {
    /** */
    public DirectDynamicProperty(
        List<String> prefix, String key, RootKey<?, ?> rootKey, DynamicConfigurationChanger changer
    ) {
        super(prefix, key, rootKey, changer);
    }

    /** {@inheritDoc} */
    @Override public T directValue() {
        return changer.getLatest(keys);
    }
}
