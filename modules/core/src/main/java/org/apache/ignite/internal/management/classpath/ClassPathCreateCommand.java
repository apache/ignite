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

package org.apache.ignite.internal.management.classpath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.classpath.IgniteClassPath;
import org.apache.ignite.internal.client.thin.TcpIgniteClient;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.NativeCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite classpath creation command.
 *
 * @see IgniteClassPath
 */
public class ClassPathCreateCommand implements NativeCommand<ClassPathCreateCommandArg, Void> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Create ClassPath instance from set of local file";
    }

    /** {@inheritDoc} */
    @Override public Class<ClassPathCreateCommandArg> argClass() {
        return ClassPathCreateCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Void execute(
        @Nullable IgniteClient client,
        @Nullable Ignite ignite,
        ClassPathCreateCommandArg arg,
        Consumer<String> printer
    ) throws Exception {
        if (client == null && !(ignite instanceof IgniteEx))
            throw new IllegalStateException("Client or IgniteEx required");

        List<Path> files = prepareFiles(arg);

        ClusterNode uploadNode = uploadNode(client, ignite);

        printer.accept("Upload node: " + uploadNode.id());

        UUID icpID = CommandUtils.execute(
            client,
            ignite,
            ClassPathStartCreationTask.class,
            arg,
            Collections.singletonList(uploadNode)
        );

        printer.accept("New ClassPath created [" +
            "uploadNode=" + uploadNode.id() + ", name=" + arg.name + ", id=" + icpID.toString() + ']');

        uploadFiles(client, ignite, printer, files, uploadNode, icpID);

        CommandUtils.execute(
            client,
            ignite,
            ClassPathDistributeTask.class,
            icpID, Collections.singletonList(uploadNode)
        );

        return null;
    }

    /** */
    private static void uploadFiles(
        @Nullable IgniteClient client,
        @Nullable Ignite ignite,
        Consumer<String> printer,
        List<Path> files,
        ClusterNode uploadNode,
        UUID icpId
    ) throws IOException {
        printer.accept("Starting to upload files:");

        // TODO: add pretty print here.
        for (Path file : files) {
            printer.accept(String.valueOf(file.toAbsolutePath()));

            if (client != null)
                ((TcpIgniteClient)client).uploadClasspathFile(uploadNode, icpId, file);
            else {
                ((IgniteEx)ignite).context().classPath().copyClassPathFileLocally(icpId, file);
            }

            printer.accept("DONE");
        }
    }

    /** */
    private static List<Path> prepareFiles(ClassPathCreateCommandArg arg) throws IOException {
        List<Path> files = new ArrayList<>(arg.files.length);
        Set<String> names = new HashSet<>();

        arg.lengths = new long[arg.files.length];

        for (int i = 0; i < arg.files.length; i++) {
            A.notEmpty(arg.files[i], "File name");

            Path f = Path.of(arg.files[i]);

            if (!Files.exists(f) || Files.isDirectory(f))
                throw new IllegalArgumentException("File not exists or directory: " + f);

            if (!names.add(f.getFileName().toString()))
                throw new IllegalArgumentException("Duplicate file name: " + f.getFileName());

            files.add(f);

            arg.lengths[i] = Files.size(f);

            // Don't want to send full path to server nodes.
            // Server nodes require files names, only.
            arg.files[i] = f.getFileName().toString();
        }

        return files;
    }

    /** */
    private static ClusterNode uploadNode(IgniteClient client, Ignite ignite) {
        if (client != null) {
            List<UUID> nodes = ((TcpIgniteClient)client).connectedToNodes();

            if (F.isEmpty(nodes))
                throw new IllegalStateException("Not connected to node");

            return client.cluster().node(F.first(nodes));
        }

        return ignite.cluster().localNode();
    }
}
