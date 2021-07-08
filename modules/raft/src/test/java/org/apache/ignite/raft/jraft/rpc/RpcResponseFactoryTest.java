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
package org.apache.ignite.raft.jraft.rpc;

import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RpcResponseFactoryTest {
    @Test
    public void testNewResponseFromStatus() {
        ErrorResponse response = (ErrorResponse) RaftRpcFactory.DEFAULT.newResponse(null, Status.OK());
        assertEquals(0, response.getErrorCode());
        assertEquals("", response.getErrorMsg());
    }

    @Test
    public void testNewResponseWithErrorStatus() {
        ErrorResponse response = (ErrorResponse) RaftRpcFactory.DEFAULT.newResponse(null,
            new Status(300, "test"));
        assertEquals(300, response.getErrorCode());
        assertEquals("test", response.getErrorMsg());
    }

    @Test
    public void testNewResponseWithVaridicArgs() {
        ErrorResponse response = (ErrorResponse) RaftRpcFactory.DEFAULT.newResponse(null, 300,
            "hello %s %d", "world", 99);
        assertEquals(300, response.getErrorCode());
        assertEquals("hello world 99", response.getErrorMsg());
    }

    @Test
    public void testNewResponseWithArgs() {
        ErrorResponse response = (ErrorResponse) RaftRpcFactory.DEFAULT.newResponse(null, 300,
            "hello world");
        assertEquals(300, response.getErrorCode());
        assertEquals("hello world", response.getErrorMsg());
    }

    @Test
    public void testNewResponseWithRaftError() {
        ErrorResponse response = (ErrorResponse) RaftRpcFactory.DEFAULT.newResponse(null, RaftError.EAGAIN,
            "hello world");
        assertEquals(response.getErrorCode(), RaftError.EAGAIN.getNumber());
        assertEquals("hello world", response.getErrorMsg());
    }
}
