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

package org.apache.ignite.configuration;

import org.apache.ignite.configuration.annotation.DirectAccess;

/**
 * Similar to a {@link ConfigurationProperty} but allows retrieving "direct" values.
 * <p>
 * "Direct" properties always return the most recent value. This makes sense in distributed scenarios, when a local
 * configuration storage may fall behind due to network lags, asynchronous logic, etc. In this case the "direct"
 * property will be retrieved from the distributed storage directly (hence the name).
 * <p>
 * For Named Lists there exists an additional guarantee: if a Named List node has been deleted and then re-created
 * under the same name, it can still be accessed using the same instance of a {@link ConfigurationProperty}, regardless
 * of the fact that it is a completely new node.
 * <p>
 * This is a hack that is intentionally made ugly to use, because it breaks the configuration abstraction and
 * essentially allows to "look into the future" of the distributed property, skipping some intermediate updates.
 *
 * @param <VIEW> Read-only view of the property value.
 * @see DirectAccess
 */
public interface DirectConfigurationProperty<VIEW> {
    /** Value of this property. */
    VIEW directValue();
}
