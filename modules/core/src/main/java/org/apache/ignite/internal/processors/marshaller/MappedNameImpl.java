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

package org.apache.ignite.internal.processors.marshaller;

import java.io.Serializable;

/**
 * Contains mapped class name and boolean flag showing whether this mapping was accepted by other nodes or not.
 */
public final class MappedNameImpl implements Serializable, MappedName {
    private static final long serialVersionUID = 3520238485773878172L;

    private final String clsName;

    private final boolean accepted;

    public MappedNameImpl(String clsName, boolean accepted) {
        this.clsName = clsName;
        this.accepted = accepted;
    }

    @Override
    public String className() {
        return clsName;
    }

    @Override
    public boolean isAccepted() {
        return accepted;
    }
}
