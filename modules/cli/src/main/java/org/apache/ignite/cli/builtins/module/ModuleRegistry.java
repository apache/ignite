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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.cli.CliPathsConfigLoader;
import org.apache.ignite.cli.IgniteCLIException;

/**
 * The registry of installed CLI or Ignite server modules.
 * Module from the the registry's point of view is a pair of (name -> [artifacts, cliArtifacts]).
 * Where:
 * <ul>
 *     <li>artifacts - is a list of Ignite server node artifacts, which will be used in classpath of any server node.</li>
 *     <li>cliArtifacts - is a list of Ignite CLI artifacts, which will be used to lookup CLI extensions.</li>
 * </ul>
 * Registry persisted on a disk as a JSON file.
 */
@Singleton
public class ModuleRegistry {
    /** Loader of CLI config with Ignite distributive paths. */
    private final CliPathsConfigLoader cliPathsCfgLdr;

    /**
     * Creates module registry instance.
     *
     * @param cliPathsCfgLdr Loader of CLI config with Ignite ditributive paths.
     */
    @Inject
    public ModuleRegistry(CliPathsConfigLoader cliPathsCfgLdr) {
        this.cliPathsCfgLdr = cliPathsCfgLdr;
    }

    /**
     * @return Path of registry file.
     */
    private Path moduleFile() {
        return cliPathsCfgLdr.loadIgnitePathsOrThrowError().installedModulesFile();
    }

    /**
     * Saves module to module registry.
     *
     * @param moduleDefinition Module definition.
     * @throws IOException If can't save the registry file.
     */
    //TODO: IGNITE-14021 write-to-tmp->move approach should be used to prevent file corruption on accidental exit
    public void saveModule(ModuleDefinition moduleDefinition) throws IOException {
        ModuleDefinitionsList moduleDefinitionsList = listInstalled();

        moduleDefinitionsList.modules.add(moduleDefinition);

        ObjectMapper objMapper = new ObjectMapper();

        objMapper.writeValue(moduleFile().toFile(), moduleDefinitionsList);
    }

    /**
     * Removes module from the registry of installed modules.
     * Note: artifacts' files will not be removed.
     * This action only remove module and its dependencies from current classpaths as a result.
     *
     * @param name Module name to remove.
     * @return true if module was removed, false otherwise.
     * @throws IOException If can't save updated registry file.
     */
    //TODO: iIGNITE-14021 write-to-tmp->move approach should be used to prevent file corruption on accidental exit
    public boolean removeModule(String name) throws IOException {
        ModuleDefinitionsList moduleDefinitionsList = listInstalled();

        boolean rmv = moduleDefinitionsList.modules.removeIf(m -> m.name.equals(name));

        ObjectMapper objMapper = new ObjectMapper();

        objMapper.writeValue(moduleFile().toFile(), moduleDefinitionsList);

        return rmv;
    }

    /**
     * @return Definitions of installed modules.
     */
    public ModuleDefinitionsList listInstalled() {
        var moduleFileAvailable =
            cliPathsCfgLdr.loadIgnitePathsConfig()
                .map(p -> p.installedModulesFile().toFile().exists())
                .orElse(false);

        if (!moduleFileAvailable)
            return new ModuleDefinitionsList(new ArrayList<>());
        else {
            ObjectMapper objMapper = new ObjectMapper();

            try {
                return objMapper.readValue(
                    moduleFile().toFile(),
                    ModuleDefinitionsList.class);
            }
            catch (IOException e) {
                throw new IgniteCLIException("Can't read lsit of installed modules because of IO error", e);
            }
        }
    }

    /**
     * Simple wrapper for a list of modules' definitions.
     * Wrap it in the form suitable for JSON serialization.
     */
    public static class ModuleDefinitionsList {
        /** Modules list. */
        public final List<ModuleDefinition> modules;

        /**
         * Creates modules definitions list.
         *
         * @param modules List of definitions to wrap.
         */
        @JsonCreator
        public ModuleDefinitionsList(
            @JsonProperty("modules") List<ModuleDefinition> modules) {
            this.modules = modules;
        }
    }

    /**
     * Definition of Ignite module. Every module can consist of server, CLI, or both artifacts' lists.
     */
    public static class ModuleDefinition {
        /** Module's name. */
        public final String name;

        /** Module's server artifacts. */
        public final List<Path> artifacts;

        /** Module's CLI artifacts. */
        public final List<Path> cliArtifacts;

        /** Type of module source. */
        public final SourceType type;

        /**
         * It can be an url, file path, or any other source identificator,
         * depending on the source type.
         */
        public final String src;

        /**
         * Creates module defition.
         *
         * @param name Module name.
         * @param artifacts Module server artifacts' paths.
         * @param cliArtifacts Module CLI artifacts' paths.
         * @param type Source type of the module.
         * @param src Source string (file path, url, maven coordinates and etc.).
         */
        @JsonCreator
        public ModuleDefinition(
            @JsonProperty("name") String name,
            @JsonProperty("artifacts") List<Path> artifacts,
            @JsonProperty("cliArtifacts") List<Path> cliArtifacts,
            @JsonProperty("type") SourceType type,
            @JsonProperty("source") String src) {
            this.name = name;
            this.artifacts = artifacts;
            this.cliArtifacts = cliArtifacts;
            this.type = type;
            this.src = src;
        }

        /**
         * @return Server artifacts' paths.
         */
        @JsonGetter("artifacts")
        public List<String> artifacts() {
            return artifacts.stream().map(a -> a.toAbsolutePath().toString()).collect(Collectors.toList());
        }

        /**
         * @return CLI artifacts paths.
         */
        @JsonGetter("cliArtifacts")
        public List<String> cliArtifacts() {
            return cliArtifacts.stream().map(a -> a.toAbsolutePath().toString()).collect(Collectors.toList());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "ModuleDefinition{" +
                "name='" + name + '\'' +
                ", artifacts=" + artifacts +
                ", cliArtifacts=" + cliArtifacts +
                ", type=" + type +
                ", source='" + src + '\'' +
                '}';
        }

    }

    /**
     * Type of module source.
     */
    public enum SourceType {
        /** Module is an maven artifact. */
        Maven,
        /** Module is an builtin module. */
        Standard
    }
}
