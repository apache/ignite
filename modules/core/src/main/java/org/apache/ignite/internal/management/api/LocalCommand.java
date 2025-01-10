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

package org.apache.ignite.internal.management.api;

import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Command that must be executed directly using {@link GridClient} or {@link Ignite} instance.
 */
public interface LocalCommand<A extends IgniteDataTransferObject, R> extends Command<A, R> {
    /**
     * Executes command logic.
     * Commands invoked in two different environments:
     * <ul>
     *     <li>CLI utility using {@link GridClient} connection to interact with cluster.</li>
     *     <li>Inside {@link Ignite} node therefore local node can be used to interact with cluster.</li>
     * </ul>
     *
     * Please, keep in mind that {@link GridClient} protocol is obsolete and can be removed in future releases.
     *
     * @param cli Optional grid client instance.
     * @param ignite Optional ignite node.
     * @param arg Command argument.
     * @param printer Results printer.
     * @return Command result.
     */
    public R execute(
        @Nullable GridClient cli,
        @Nullable IgniteClient client,
        @Nullable Ignite ignite,
        A arg,
        Consumer<String> printer
    ) throws Exception;
}
