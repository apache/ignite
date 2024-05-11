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

package org.apache.ignite.internal.management.snapshot;

import java.util.function.Consumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.ComputeCommand;

/** */
public abstract class AbstractSnapshotCommand<A extends IgniteDataTransferObject>
    implements ComputeCommand<A, SnapshotTaskResult> {
    /** {@inheritDoc} */
    @Override public void printResult(A arg, SnapshotTaskResult res, Consumer<String> printer) {
        try {
            printer.accept(String.valueOf(res.result()));
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }
}
