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
package org.apache.ignite.raft.jraft.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RetryAgainException;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.GetFileRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.GetFileResponse;
import org.apache.ignite.raft.jraft.storage.io.FileReader;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.apache.ignite.raft.jraft.util.OnlyForTest;
import org.apache.ignite.raft.jraft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File reader service.
 */
public final class FileService {
    private static final Logger LOG = LoggerFactory.getLogger(FileService.class);

    private static final FileService INSTANCE = new FileService(); // TODO asch fixme IGNITE-14832

    private final ConcurrentMap<Long, FileReader> fileReaderMap = new ConcurrentHashMap<>();
    private final AtomicLong nextId = new AtomicLong();

    /**
     * Retrieve the singleton instance of FileService.
     *
     * @return a fileService instance
     */
    public static FileService getInstance() {
        return INSTANCE;
    }

    @OnlyForTest
    void clear() {
        this.fileReaderMap.clear();
    }

    private FileService() {
        final long processId = Utils.getProcessId(ThreadLocalRandom.current().nextLong(10000, Integer.MAX_VALUE));
        final long initialValue = Math.abs(processId << 45 | System.nanoTime() << 17 >> 17);
        this.nextId.set(initialValue);
        LOG.info("Initial file reader id in FileService is {}", initialValue);
    }

    /**
     * Handle GetFileRequest, run the response or set the response with done.
     */
    public Message handleGetFile(final GetFileRequest request, final RpcRequestClosure done) {
        if (request.getCount() <= 0 || request.getOffset() < 0) {
            return RaftRpcFactory.DEFAULT //
                .newResponse(GetFileResponse.getDefaultInstance(), RaftError.EREQUEST, "Invalid request: %s", request);
        }
        final FileReader reader = this.fileReaderMap.get(request.getReaderId());

        if (LOG.isDebugEnabled()) {
            LOG.info("handleGetFile id={}, name={}, offset={}, cnt={}",
                request.getReaderId(), request.getFilename(), request.getOffset(), request.getCount());
        }

        if (reader == null) {
            return RaftRpcFactory.DEFAULT //
                .newResponse(GetFileResponse.getDefaultInstance(), RaftError.ENOENT, "Fail to find reader=%d",
                    request.getReaderId());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("GetFile from {} path={} filename={} offset={} count={}", done.getRpcCtx().getRemoteAddress(),
                reader.getPath(), request.getFilename(), request.getOffset(), request.getCount());
        }

        final ByteBufferCollector dataBuffer = ByteBufferCollector.allocate();
        final GetFileResponse.Builder responseBuilder = GetFileResponse.newBuilder();
        try {
            final int read = reader
                .readFile(dataBuffer, request.getFilename(), request.getOffset(), request.getCount());
            responseBuilder.setReadSize(read);
            responseBuilder.setEof(read == FileReader.EOF);
            final ByteBuffer buf = dataBuffer.getBuffer();
            buf.flip();
            if (!buf.hasRemaining()) {
                // skip empty data
                responseBuilder.setData(ByteString.EMPTY);
            }
            else {
                // TODO check hole https://issues.apache.org/jira/browse/IGNITE-14832
                responseBuilder.setData(new ByteString(buf));
            }
            return responseBuilder.build();
        }
        catch (final RetryAgainException e) {
            return RaftRpcFactory.DEFAULT //
                .newResponse(GetFileResponse.getDefaultInstance(), RaftError.EAGAIN,
                    "Fail to read from path=%s filename=%s with error: %s", reader.getPath(), request.getFilename(),
                    e.getMessage());
        }
        catch (final IOException e) {
            LOG.error("Fail to read file path={} filename={}", reader.getPath(), request.getFilename(), e);
            return RaftRpcFactory.DEFAULT //
                .newResponse(GetFileResponse.getDefaultInstance(), RaftError.EIO,
                    "Fail to read from path=%s filename=%s", reader.getPath(), request.getFilename());
        }
    }

    /**
     * Adds a file reader and return it's generated readerId.
     */
    public long addReader(final FileReader reader) {
        final long readerId = this.nextId.getAndIncrement();
        if (this.fileReaderMap.putIfAbsent(readerId, reader) == null) {
            return readerId;
        }
        else {
            return -1L;
        }
    }

    /**
     * Remove the reader by readerId.
     */
    public boolean removeReader(final long readerId) {
        return this.fileReaderMap.remove(readerId) != null;
    }
}
