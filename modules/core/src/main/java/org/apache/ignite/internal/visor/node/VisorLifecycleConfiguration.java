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

package org.apache.ignite.internal.visor.node;

import java.io.Serializable;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactArray;

/**
 * Data transfer object for node lifecycle configuration properties.
 */
public class VisorLifecycleConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Lifecycle beans. */
    private String beans;

    /**
     * @param c Grid configuration.
     * @return Data transfer object for node lifecycle configuration properties.
     */
    public static VisorLifecycleConfiguration from(IgniteConfiguration c) {
        VisorLifecycleConfiguration cfg = new VisorLifecycleConfiguration();

        cfg.beans = compactArray(c.getLifecycleBeans());

        return cfg;
    }

    /**
     * @return Lifecycle beans.
     */
    @Nullable public String beans() {
        return beans;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorLifecycleConfiguration.class, this);
    }
}