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

package org.apache.ignite.internal.processors.hadoop.igfs;

import java.io.IOException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Extended IGFS server interface.
 */
public interface HadoopIgfsEx extends HadoopIgfs {
    /**
     * Adds event listener that will be invoked when connection with server is lost or remote error has occurred.
     * If connection is closed already, callback will be invoked synchronously inside this method.
     *
     * @param delegate Stream delegate.
     * @param lsnr Event listener.
     */
    public void addEventListener(HadoopIgfsStreamDelegate delegate, HadoopIgfsStreamEventListener lsnr);

    /**
     * Removes event listener that will be invoked when connection with server is lost or remote error has occurred.
     *
     * @param delegate Stream delegate.
     */
    public void removeEventListener(HadoopIgfsStreamDelegate delegate);

    /**
     * Asynchronously reads specified amount of bytes from opened input stream.
     *
     * @param delegate Stream delegate.
     * @param pos Position to read from.
     * @param len Data length to read.
     * @param outBuf Optional output buffer. If buffer length is less then {@code len}, all remaining
     *     bytes will be read into new allocated buffer of length {len - outBuf.length} and this buffer will
     *     be the result of read future.
     * @param outOff Output offset.
     * @param outLen Output length.
     * @return Read data.
     */
    public IgniteInternalFuture<byte[]> readData(HadoopIgfsStreamDelegate delegate, long pos, int len,
        @Nullable final byte[] outBuf, final int outOff, final int outLen);

    /**
     * Writes data to the stream with given streamId. This method does not return any future since
     * no response to write request is sent.
     *
     * @param delegate Stream delegate.
     * @param data Data to write.
     * @param off Offset.
     * @param len Length.
     * @throws IOException If failed.
     */
    public void writeData(HadoopIgfsStreamDelegate delegate, byte[] data, int off, int len) throws IOException;

    /**
     * Close server stream.
     *
     * @param delegate Stream delegate.
     * @throws IOException If failed.
     */
    public void closeStream(HadoopIgfsStreamDelegate delegate) throws IOException;

    /**
     * Flush output stream.
     *
     * @param delegate Stream delegate.
     * @throws IOException If failed.
     */
    public void flush(HadoopIgfsStreamDelegate delegate) throws IOException;

    /**
     * The user this Igfs instance works on behalf of.
     * @return the user name.
     */
    public String user();
}