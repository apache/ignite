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

package org.apache.ignite.internal.processors.hadoop.taskexecutor.external;

import java.util.Collection;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * External task metadata (classpath, JVM options) needed to start external process execution.
 */
public class HadoopExternalTaskMetadata {
    /** Process classpath. */
    private Collection<String> classpath;

    /** JVM options. */
    @GridToStringInclude
    private Collection<String> jvmOpts;

    /**
     * @return JVM Options.
     */
    public Collection<String> jvmOptions() {
        return jvmOpts;
    }

    /**
     * @param jvmOpts JVM options.
     */
    public void jvmOptions(Collection<String> jvmOpts) {
        this.jvmOpts = jvmOpts;
    }

    /**
     * @return Classpath.
     */
    public Collection<String> classpath() {
        return classpath;
    }

    /**
     * @param classpath Classpath.
     */
    public void classpath(Collection<String> classpath) {
        this.classpath = classpath;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopExternalTaskMetadata.class, this);
    }
}