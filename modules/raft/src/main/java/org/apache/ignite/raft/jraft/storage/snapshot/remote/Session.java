/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.storage.snapshot.remote;

import java.io.Closeable;
import org.apache.ignite.raft.jraft.Status;

/**
 * A copy session.
 */
public interface Session extends Closeable {

    /**
     * Cancel the copy job.
     */
    void cancel();

    /**
     * Block the thread to wait the copy job finishes or canceled.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    void join() throws InterruptedException;

    /**
     * Returns the copy job status.
     */
    Status status();
}
