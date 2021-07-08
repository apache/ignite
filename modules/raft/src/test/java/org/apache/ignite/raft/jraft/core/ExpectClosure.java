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
package org.apache.ignite.raft.jraft.core;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.RaftError;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExpectClosure implements Closure {
    private int expectedErrCode;
    private String expectErrMsg;
    private CountDownLatch latch;

    public ExpectClosure(CountDownLatch latch) {
        this(RaftError.SUCCESS, latch);
    }

    public ExpectClosure(RaftError expectedErrCode, CountDownLatch latch) {
        this(expectedErrCode, null, latch);

    }

    public ExpectClosure(RaftError expectedErrCode, String expectErrMsg, CountDownLatch latch) {
        super();
        this.expectedErrCode = expectedErrCode.getNumber();
        this.expectErrMsg = expectErrMsg;
        this.latch = latch;
    }

    public ExpectClosure(int code, String expectErrMsg, CountDownLatch latch) {
        super();
        this.expectedErrCode = code;
        this.expectErrMsg = expectErrMsg;
        this.latch = latch;
    }

    @Override
    public void run(Status status) {
        if (this.expectedErrCode >= 0) {
            assertEquals(this.expectedErrCode, status.getCode());
        }
        if (this.expectErrMsg != null) {
            assertEquals(this.expectErrMsg, status.getErrorMsg());
        }
        latch.countDown();
    }

}
