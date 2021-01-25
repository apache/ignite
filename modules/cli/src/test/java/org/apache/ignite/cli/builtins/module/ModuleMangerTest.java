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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import javax.inject.Inject;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.ignite.cli.IgnitePaths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import picocli.CommandLine;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for CLI module manager logic.
 */
@ExtendWith(MockitoExtension.class)
@MicronautTest
public class ModuleMangerTest {
    /** */
    @Inject
    MavenArtifactResolver mavenArtifactRslvr;

    /** */
    @Inject
    ModuleManager moduleMgr;

    /** */
    @Inject
    ModuleRegistry moduleRegistry;

    /** */
    @TempDir
    Path artifactsDir;

    /** Temporary home directory replacement. */
    @TempDir
    Path homeDir;

    /** */
    @Test
    void testCliModuleInstallation() throws IOException {
       var rootArtifact = generateJar("test-module", "1.0", true);
       var depArtifact = generateJar("dep-artifact", "0.1", false);

       when(mavenArtifactRslvr.resolve(any(), any(), any(), any(), any())).thenReturn(
           new ResolveResult(Arrays.asList(rootArtifact, depArtifact)));

       var ignitePaths = new IgnitePaths(homeDir.resolve("bin"), homeDir.resolve("work"), "n/a");

       moduleMgr.setOut(new PrintWriter(System.out));
       moduleMgr.setColorScheme(CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO));
       moduleMgr.addModule("mvn:any-group:test-module:1.0", ignitePaths, Collections.emptyList());

       verify(moduleRegistry).saveModule(argThat(m ->
           m.cliArtifacts.equals(Arrays.asList(rootArtifact, depArtifact)) &&
                m.artifacts.equals(Collections.emptyList())));
    }

    /** */
    @Test
    void testServerModuleInstallation() throws IOException {
        var rootArtifact = generateJar("test-module", "1.0", false);
        var depArtifact = generateJar("dep-artifact", "0.1", false);

        when(mavenArtifactRslvr.resolve(any(), any(), any(), any(), any())).thenReturn(
            new ResolveResult(Arrays.asList(rootArtifact, depArtifact)));

        var ignitePaths = new IgnitePaths(homeDir.resolve("bin"), homeDir.resolve("work"), "n/a");

        moduleMgr.setOut(new PrintWriter(System.out));
        moduleMgr.setColorScheme(CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO));
        moduleMgr.addModule("mvn:any-group:test-module:1.0", ignitePaths, Collections.emptyList());

        verify(moduleRegistry).saveModule(argThat(m ->
            m.artifacts.equals(Arrays.asList(rootArtifact, depArtifact)) &&
                m.cliArtifacts.equals(Collections.emptyList())));
    }

    /** */
    @MockBean(MavenArtifactResolver.class)
    MavenArtifactResolver mavenArtifactResolver() {
        return mock(MavenArtifactResolver.class);
    }

    /** */
    @MockBean(ModuleRegistry.class)
    ModuleRegistry moduleStorage() {
        return mock(ModuleRegistry.class);
    }

    /**
     * Generates jar file with CLI module mark or not.
     */
    private Path generateJar(String artifactId, String ver, boolean isCliModule) throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");

        var jarPath = artifactsDir.resolve(MavenArtifactResolver.fileNameByArtifactPattern(artifactId, ver));

        if (isCliModule)
            manifest.getMainAttributes().put(new Attributes.Name(ModuleManager.CLI_MODULE_MANIFEST_HEADER), "true");

        var target = new JarOutputStream(new FileOutputStream(jarPath.toString()), manifest);
        target.close();

        return jarPath;
    }
}
