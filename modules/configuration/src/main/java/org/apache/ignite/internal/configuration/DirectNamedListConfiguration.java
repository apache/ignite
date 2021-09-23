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

import java.util.List;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.DirectConfigurationProperty;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.DirectAccess;

/**
 * {@link NamedListConfiguration} extension that implements {@link DirectConfigurationProperty}.
 *
 * @see DirectAccess
 */
public class DirectNamedListConfiguration<T extends ConfigurationProperty<VIEW>, VIEW, CHANGE extends VIEW>
    extends NamedListConfiguration<T, VIEW, CHANGE>
    implements DirectConfigurationProperty<VIEW> {
    /** */
    public DirectNamedListConfiguration(
        List<String> prefix,
        String key,
        RootKey<?, ?> rootKey,
        DynamicConfigurationChanger changer,
        BiFunction<List<String>, String, T> creator
    ) {
        super(prefix, key, rootKey, changer, creator);
    }

    /** {@inheritDoc} */
    @Override public VIEW directValue() {
         return changer.getLatest(keys);
    }
}
