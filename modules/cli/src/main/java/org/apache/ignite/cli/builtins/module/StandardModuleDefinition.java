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

package org.apache.ignite.cli.builtins.module;

import java.util.Collections;
import java.util.List;

/**
 * Definition of Ignite standard module.
 * Every module has two artifacts' lists - one for server modules
 * and one for CLI tool extensions, if any.
 */
public class StandardModuleDefinition {
    /** Module name. **/
    public final String name;

    /** Module description. */
    public final String desc;

    /** List of server artifacts. */
    public final List<String> artifacts;

    /** List of CLI tool artifacts. */
    public final List<String> cliArtifacts;

    /**
     * Creates definition for standard Ignite module.
     *
     * @param name Module name.
     * @param desc Module description.
     * @param artifacts Server artifacts.
     * @param cliArtifacts CLI tool artifacts.
     */
    public StandardModuleDefinition(String name, String desc, List<String> artifacts, List<String> cliArtifacts) {
        this.name = name;
        this.desc = desc;
        this.artifacts = Collections.unmodifiableList(artifacts);
        this.cliArtifacts = Collections.unmodifiableList(cliArtifacts);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name + ":\t" + desc;
    }
}
