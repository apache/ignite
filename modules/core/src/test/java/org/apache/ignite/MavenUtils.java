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

package org.apache.ignite;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Provides some useful methods to work with Maven.
 */
public class MavenUtils {
    /** Path to Maven local repository. For caching. */
    private static String locRepPath = null;

    /**
     * Gets a path to an artifact with given version
     * and groupId=org.apache.ignite and artifactId=ignite-core.
     *
     * At first, artifact is looked for in the Maven local repository,
     * if it isn't exists there, it will be downloaded and stored via Maven.
     *
     * @param ver Version of ignite-core artifact.
     * @return Path to the artifact.
     * @throws Exception In case of an error.
     * @see #getPathToArtifact(String)
     */
    public static String getPathToIgniteCoreArtifact(String ver) throws Exception {
        return getPathToArtifact("org.apache.ignite:ignite-core:" + ver);
    }

    /**
     * Gets a path to an artifact with given identifier.
     *
     * At first, artifact is looked for in the Maven local repository,
     * if it isn't exists there, it will be downloaded and stored via Maven.
     *
     * @param artifact Artifact identifier, must match pattern [groupId:artifactId:version].
     * @return Path to the artifact.
     * @throws Exception In case of an error.
     */
    public static String getPathToArtifact(String artifact) throws Exception {
        String[] names = artifact.split(":");

        assert names.length == 3;

        String groupId = names[0];
        String artifactId = names[1];
        String version = names[2];

        String jarFileName = String.format("%s-%s.jar", artifactId, version);

        String pathToArtifact = getMavenLocalRepositoryPath() + File.separator +
            groupId.replace(".", File.separator) + File.separator +
            artifactId.replace(".", File.separator) + File.separator +
            version + File.separator + jarFileName;

        if (Files.notExists(Paths.get(pathToArtifact)))
            downloadArtifact(artifact);

        return pathToArtifact;
    }

    /**
     * Gets a path to the Maven local repository.
     *
     * @return Path to the Maven local repository.
     * @throws Exception In case of an error.
     */
    public static String getMavenLocalRepositoryPath() throws Exception {
        if (locRepPath == null)
            locRepPath = defineMavenLocalRepositoryPath();

        return locRepPath;
    }

    /**
     * Defines a path to the Maven local repository.
     *
     * @return Path to the Maven local repository.
     * @throws Exception In case of an error.
     */
    private static String defineMavenLocalRepositoryPath() throws Exception {
        Process p = exec("mvn help:effective-settings");

        String output = CharStreams.toString(new InputStreamReader(p.getInputStream(), Charsets.UTF_8));

        int endTagPos = output.indexOf("</localRepository>");

        assert endTagPos >= 0 : "Couldn't define path to Maven local repository";

        return output.substring(output.lastIndexOf(">", endTagPos) + 1, endTagPos);
    }

    /**
     * Downloads and stores in local repository an artifact with given identifier.
     *
     * @param artifact Artifact identifier, must match pattern [groupId:artifactId:version].
     * @throws Exception In case of an error.
     */
    private static void downloadArtifact(String artifact) throws Exception {
        X.println("Downloading artifact... Identifier: " + artifact);

        exec("mvn dependency:get -Dartifact=" + artifact);

        X.println("Download is finished");
    }

    /**
     * Executes given command in operation system.
     *
     * @param cmd Command to execute.
     * @return Process of executed command.
     * @throws Exception In case of an error.
     */
    private static Process exec(String cmd) throws Exception {
        ProcessBuilder pb = new ProcessBuilder();
        pb.redirectErrorStream(true);

        pb.command(U.isWindows() ?
            new String[] {"cmd", "/c", cmd} :
            new String[] {"/bin/bash", "-c", cmd});

        final Process p = pb.start();

        ExecutorService executor = Executors.newSingleThreadExecutor();

        Future<Integer> fut = executor.submit(new Callable<Integer>() {
            @Override public Integer call() throws Exception {
                return p.waitFor();
            }
        });

        try {
            int exitVal = fut.get(5, TimeUnit.MINUTES);

            if (exitVal != 0)
                throw new Exception(String.format("Abnormal exit value of %s for pid %s", exitVal, U.jvmPid()));
        }
        catch (Exception e) {
            p.destroy();

            X.printerrln("Command='" + cmd + "' couldn't be executed: "
                + CharStreams.toString(new InputStreamReader(p.getInputStream(), Charsets.UTF_8)), e);

            throw e;
        }

        return p;
    }
}
