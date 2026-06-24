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

package org.apache.ignite.internal.classpath;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.classpath.ClassPathProcessor.metastorageKey;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** */
public class ClassPathTestUtils {
    /** */
    public static Set<Path> files() throws IOException {
        return Stream.of(
            file(0), file(1025), file(U.MB), file(2 * U.MB), file(2 * U.MB + 42)).collect(Collectors.toSet()
        );
    }

    /** */
    public static Set<String> fileNames(Set<Path> dirs) {
        return dirs.stream().map(Path::getFileName).map(Path::toString).collect(Collectors.toSet());
    }

    /** */
    public static String[] fileArg(Set<Path> cpFiles) {
        String[] files = new String[cpFiles.size()];

        int i = 0;

        for (Path p : cpFiles)
            files[i++] = p.toAbsolutePath().toString();

        return files;
    }

    /** */
    public static Path file(long size) throws IOException {
        File f = File.createTempFile(size + "_bytes", ".temp");

        f.deleteOnExit();

        try (OutputStream os = Files.newOutputStream(f.toPath())) {
            long written = 0;

            byte[] batch = new byte[4 * (int)U.KB];

            while (written < size) {
                ThreadLocalRandom.current().nextBytes(batch);

                int toWrite = (int)Math.min(size - written, batch.length);

                os.write(batch, 0, toWrite);

                written += toWrite;
            }
        }

        assert f.length() == size;

        return f.toPath();
    }

    /** */
    public static void checkFilesExists(IgniteEx node, String cpName, Set<Path> cpFiles) throws IOException {
        NodeFileTree ft = node.context().pdsFolderResolver().fileTree();

        Set<Path> nodeCpFiles = Files.list(ft.classPathRoot(cpName).toPath()).collect(Collectors.toSet());

        Set<String> nodeCpFilesNames = fileNames(nodeCpFiles);

        nodeCpFilesNames.remove(NodeFileTree.ICP_DESCRIPTOR_NAME);

        assertEquals(
            "Files must be deployed on each node",
            fileNames(cpFiles),
            nodeCpFilesNames
        );

        for (Path cpFile : cpFiles) {
            Path nodeFile = nodeCpFiles.stream()
                .filter(p -> Objects.equals(p.getFileName().toString(), cpFile.getFileName().toString()))
                .findFirst().get();

            assertEquals(Files.size(cpFile), Files.size(nodeFile));
        }
    }

    /** */
    public static void checkDeployedOn(IgniteEx srv, String cpName) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(() -> {
            try {
                return srv.context().distributedMetastorage().<IgniteClassPath>read(metastorageKey(cpName))
                    .deployedOnNodes().contains(srv.localNode().id());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }, 10_000));
    }


    /** */
    static ClassPathFilesTransmissionHandler transmissionHandler(IgniteEx grid1) {
        return getFieldValue(grid1.context().classPath(), "icpFilesHnd");
    }
}
