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
import org.jetbrains.annotations.NotNull;

/**
 * Inner interface to describe public property that can be managed by command line interface.
 * See more {@code CommandHandler} and {@code PropertyCommand}.
 */
public interface PublicProperty <T extends Serializable>  {
    /**
     * @return Property description.
     */
    @NotNull String description();

    /**
     * @param str Text representation of the property value.
     * @return Property value.
     * @throws IllegalArgumentException On parse error.
     */
    T parse(String str);

    /**
     * @param val Property value.
     * @return Text representation of the property value.
     */
    String format(T val);

    /**
     * @return Permission required to read property value.
     */
    SecurityPermission readPermission();

    /**
     * @return Permission required to write property value.
     */
    SecurityPermission writePermission();
}
