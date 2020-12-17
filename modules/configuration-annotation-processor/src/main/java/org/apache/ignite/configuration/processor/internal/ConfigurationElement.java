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

package org.apache.ignite.configuration.processor.internal;

import com.squareup.javapoet.TypeName;

/**
 * Element of configuration.
 */
public class ConfigurationElement {
    /** Name of configuration element. */
    private final String name;

    /** Configuration type. */
    private final TypeName type;

    /** Configuration VIEW type. */
    private final TypeName view;

    /** Configuration INIT type. */
    private final TypeName init;

    /** Configuration CHANGE type. */
    private final TypeName change;

    /** Constructor. */
    public ConfigurationElement(TypeName type, String name, TypeName view, TypeName init, TypeName change) {
        this.type = type;
        this.name = name;
        this.view = view;
        this.init = init;
        this.change = change;
    }

    /** */
    public String getName() {
        return name;
    }

    /** */
    public TypeName getType() {
        return type;
    }

    /** */
    public TypeName getView() {
        return view;
    }

    /** */
    public TypeName getInit() {
        return init;
    }

    /** */
    public TypeName getChange() {
        return change;
    }
}
