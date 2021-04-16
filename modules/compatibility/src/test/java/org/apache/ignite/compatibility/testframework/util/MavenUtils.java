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

package org.apache.ignite.compatibility.testframework.util;

import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Provides some useful methods to work with Maven.
 */
public class MavenUtils {
    /** Path to Maven local repository. For caching. */
    private static String locRepPath = null;

    /** */
    private static final String GG_MVN_REPO = "http://www.gridgainsystems.com/nexus/content/repositories/external";

    /** Set this flag to true if running PDS compatibility tests locally. */
    private static boolean useGgRepo;

    /**
     * Gets a path to an artifact with given version and groupId=org.apache.ignite and artifactId={@code artifactId}.
     * <br>
     * At first, artifact is looked for in the Maven local repository, if it isn't exists there, it will be downloaded
     * and stored via Maven.
     * <br>
     *
     * @param groupId group name, e.g. 'org.apache.ignite'.
     * @param ver Version of ignite or 3rd party library artifact.
     * @param classifier Artifact classifier.
     * @return Path to the artifact.
     * @throws Exception In case of an error.
     * @see #getPathToArtifact(String)
     */
    public static String getPathToIgniteArtifact(@NotNull String groupId,
        @NotNull String artifactId, @NotNull String ver,
        @Nullable String classifier) throws Exception {
        String artifact = groupId +
            ":" + artifactId + ":" + ver;

        if (classifier != null)
            artifact += ":jar:" + classifier;

        return getPathToArtifact(artifact);
    }

    /**
     * Gets a path to an artifact with given identifier.
     *
     * At first, artifact is looked for in the Maven local repository, if it isn't exists there, it will be downloaded
     * and stored via Maven.
     *
     * @param artifact Artifact identifier, must match pattern [groupId:artifactId:version[:packaging[:classifier]]].
     * @return Path to the artifact.
     * @throws Exception In case of an error.
     */
    public static String getPathToArtifact(@NotNull String artifact) throws Exception {
        String[] names = artifact.split(":");

        assert names.length >= 3;

        String groupId = names[0];
        String artifactId = names[1];
        String version = names[2];
        String packaging = names.length > 3 ? names[3] : null;
        String classifier = names.length > 4 ? names[4] : null;

        String jarFileName = String.format("%s-%s%s.%s",
            artifactId,
            version,
            (classifier == null ? "" : "-" + classifier),
            (packaging == null ? "jar" : packaging)
        );

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
        String output = exec(buildMvnCommand() + " help:effective-settings");

        int endTagPos = output.indexOf("</localRepository>");

        assert endTagPos >= 0 : "Couldn't define path to Maven local repository";

        return output.substring(output.lastIndexOf('>', endTagPos) + 1, endTagPos);
    }

    /**
     * Downloads and stores in local repository an artifact with given identifier.
     *
     * @param artifact Artifact identifier, must match pattern [groupId:artifactId:version].
     * @throws Exception In case of an error.
     */
    private static void downloadArtifact(String artifact) throws Exception {
        X.println("Downloading artifact... Identifier: " + artifact);

        // Default platform independ path for maven settings file.
        Path localProxyMavenSettings = Paths.get(System.getProperty("user.home"), ".m2", "local-proxy.xml");

        String localProxyMavenSettingsFromEnv = System.getenv("LOCAL_PROXY_MAVEN_SETTINGS");

        SB mavenCommandArgs = new SB(" org.apache.maven.plugins:maven-dependency-plugin:3.0.2:get -Dartifact=" + artifact);

        if (!F.isEmpty(localProxyMavenSettingsFromEnv))
            localProxyMavenSettings = Paths.get(localProxyMavenSettingsFromEnv);

        if (Files.exists(localProxyMavenSettings))
            mavenCommandArgs.a(" -s " + localProxyMavenSettings.toString());
        else
            mavenCommandArgs.a(useGgRepo ? " -DremoteRepositories=" + GG_MVN_REPO : "");

        exec(buildMvnCommand() + mavenCommandArgs.toString());

        X.println("Download is finished");
    }

    /**
     * Executes given command in operation system.
     *
     * @param cmd Command to execute.
     * @return Output of result of executed command.
     * @throws Exception In case of an error.
     */
    private static String exec(String cmd) throws Exception {
        ProcessBuilder pb = new ProcessBuilder();
        pb.redirectErrorStream(true);

        pb.command(U.isWindows() ?
            new String[] {"cmd", "/c", cmd} :
            new String[] {"/bin/bash", "-c", cmd});

        final Process p = pb.start();

        Future<String> fut = Executors.newSingleThreadExecutor().submit(new Callable<String>() {
            @Override public String call() throws Exception {
                String output = CharStreams.toString(new InputStreamReader(p.getInputStream(), Charsets.UTF_8));

                p.waitFor();

                return output;
            }
        });

        String output = null;

        try {
            output = fut.get(5, TimeUnit.MINUTES);

            int exitVal = p.exitValue();

            if (exitVal != 0)
                throw new Exception(String.format("Abnormal exit value of %s for pid %s", exitVal, U.jvmPid()));

            return output;
        }
        catch (Exception e) {
            p.destroy();

            X.printerrln("Command='" + cmd + "' couldn't be executed: " + output, Charsets.UTF_8, e);

            throw e;
        }
    }

    /**
     * @return Maven executable command.
     */
    private static String buildMvnCommand() {
        String m2Home = System.getenv("M2_HOME");

        if (m2Home == null)
            m2Home = System.getProperty("M2_HOME");

        if (m2Home == null)
            return "mvn";

        return m2Home + "/bin/mvn";
    }
}
