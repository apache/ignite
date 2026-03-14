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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.client.thin.TcpIgniteClient;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.NativeCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 *
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
        if (client != null) {
            create(client, arg, printer);

            return null;
        }

        throw new UnsupportedOperationException("Creating with the Ignite instance not supported at a time.");
    }

    /** */
    private void create(@Nullable IgniteClient client, ClassPathCreateCommandArg arg, Consumer<String> printer) throws Exception {
        TcpIgniteClient cli = (TcpIgniteClient)client;

        List<Path> files = new ArrayList<>(arg.files.length);
        long[] lengths = new long[arg.files.length];

        for (int i = 0; i < arg.files.length; i++) {
            Path f = Path.of(arg.files[i]);

            if (!Files.exists(f))
                throw new IllegalArgumentException("File not exists: " + f);

            files.add(f);

            lengths[i] = Files.size(f);
        }

        arg.lengths = lengths;
        // We don't want to send full path to server nodes.
        // Server nodes require files names, only.
        arg.files = fileNames(files);

        ClusterNode uploadNode = uploadNode(client);

        printer.accept("Upload node: " + uploadNode.id());

        UUID icpID = CommandUtils.execute(client, null, ClassPathStartCreationTask.class, arg, Collections.singletonList(uploadNode));

        printer.accept("New classpath registered [uploadNode=" + uploadNode.id() + ", name=" + arg.name + ", id=" + icpID.toString() + ']');
        printer.accept("Starting to upload files:");

        for (Path file : files) {
            printer.accept(String.valueOf(file.toAbsolutePath()));
            cli.uploadClasspathFile(uploadNode, icpID, file);
            printer.accept("DONE");
        }
    }

    /** */
    private static ClusterNode uploadNode(IgniteClient client) {
        Collection<ClusterNode> nodes = CommandUtils.nodes(client, null);

        if (F.isEmpty(nodes))
            throw new IgniteException("Cluster empty");

        Collection<ClusterNode> servers = CommandUtils.servers(nodes);

        if (F.isEmpty(servers))
            throw new IgniteException("No server nodes");

        return F.first(servers);
    }

    /** */
    private static String[] fileNames(List<Path> files) {
        String[] fileNames = new String[files.size()];

        for (int i = 0; i < fileNames.length; i++)
            fileNames[i] = files.get(i).getFileName().toString();

        return fileNames;
    }
}
