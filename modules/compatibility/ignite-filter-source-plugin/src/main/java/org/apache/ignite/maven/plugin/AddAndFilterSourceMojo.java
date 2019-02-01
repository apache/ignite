/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.maven.plugin;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

@Mojo(name = "add-and-filter-source", defaultPhase = LifecyclePhase.GENERATE_SOURCES, threadSafe = true)
public class AddAndFilterSourceMojo extends AbstractMojo {
    @Parameter(required = true)
    private String version;

    @Parameter(required = true)
    private File[] sources;

    @Parameter( readonly = true, defaultValue = "${project}")
    private MavenProject project;

    @Inject
    private FileVersionChecker fileVersionChecker;

    /**
     * Executes mojo.
     *
     * TODO: Introduce new mojo that will clean-up shared source folder after compilation.
     *
     * @throws MojoExecutionException If failed.
     */
    public void execute() throws MojoExecutionException {
        try {
            File sharedSourcesDir = prepareSharedSourcesDirectory();

            for (int i = 0; i < sources.length; i++) {
                File sourceDir = sources[i];
                File targetDir = new File(sharedSourcesDir, "source-" + i);

                // Deep-copy all sources to separate directory.
                FileUtils.copyDirectory(sourceDir, targetDir);

                List<File> notSatisfied = findFilesNotSatisfiedVersion(targetDir, version);

                // Delete such files.
                for (File file : notSatisfied)
                    Files.delete(file.toPath());

                project.addCompileSourceRoot(targetDir.getAbsolutePath());
            }
        }
        catch (IOException e) {
            throw new MojoExecutionException("Failed to copy and filter sources", e);
        }
    }

    /**
     *
     */
    private File prepareSharedSourcesDirectory() throws IOException {
        File projectRoot = project.getFile().getParentFile();

        File sharedSourcesDir = new File(projectRoot, "shared-sources");

        if (Files.exists(sharedSourcesDir.toPath()))
            FileUtils.deleteDirectory(sharedSourcesDir);

        Files.createDirectory(sharedSourcesDir.toPath());

        return sharedSourcesDir;
    }

    /**
     * @param rootDir Root directory.
     * @param version Version.
     */
    private List<File> findFilesNotSatisfiedVersion(File rootDir, String version) throws IOException {
        List<File> filtered = new ArrayList<>();

        Files.walkFileTree(
            rootDir.toPath(),
            new SimpleFileVisitor<Path>() {
                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.toFile().getAbsolutePath().endsWith(".java")) {
                        getLog().info("Processing: " + file.toFile().getAbsolutePath());

                        if (!fileVersionChecker.satisfiesVersion(file.toFile(), version)) {
                            filtered.add(file.toFile());

                            getLog().info("Excluded: " + file.toFile().getAbsolutePath());
                        }
                    }

                    return FileVisitResult.CONTINUE;
                }
        });

        return filtered;
    }
}
