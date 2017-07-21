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

package org.apache.ignite.internal.util.ipc;

import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.ignite.IgniteCheckedException;

/**
 * IGFS IPC endpoint used for point-to-point communication.
 */
public interface IpcEndpoint extends Closeable {
    /**
     * Gets input stream associated with this IPC endpoint.
     *
     * @return IPC input stream.
     * @throws IgniteCheckedException If error occurred.
     */
    public InputStream inputStream() throws IgniteCheckedException;

    /**
     * Gets output stream associated with this IPC endpoint.
     *
     * @return IPC output stream.
     * @throws IgniteCheckedException If error occurred.
     */
    public OutputStream outputStream() throws IgniteCheckedException;

    /**
     * Closes endpoint. Note that IPC endpoint may acquire native resources so it must be always closed
     * once it is not needed.
     */
    @Override public void close();
}