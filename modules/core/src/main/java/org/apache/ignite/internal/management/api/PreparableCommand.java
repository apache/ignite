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
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/** */
public interface PreparableCommand<A extends IgniteDataTransferObject, R> extends Command<A, R> {
    /**
     * Enriches argument with cluster information if required.
     *
     * @param cli Grid client to get required information from cluster.
     * @param arg Command argument.
     * @param printer Implementation specific printer.
     * @return {@code True} if command must be executed, {@code false} otherwise.
     * @throws GridClientException If failed.
     */
    public boolean prepare(GridClient cli, A arg, Consumer<String> printer) throws GridClientException;
}
