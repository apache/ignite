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
package org.apache.ignite.raft.jraft.util;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * CountDown event.
 */
public class CountDownEvent {

    private int state = 0;
    private final Lock lock = new ReentrantLock();
    private final Condition busyCond = this.lock.newCondition();
    private volatile Object attachment;

    public Object getAttachment() {
        return this.attachment;
    }

    public void setAttachment(final Object attachment) {
        this.attachment = attachment;
    }

    public int incrementAndGet() {
        this.lock.lock();
        try {
            return ++this.state;
        }
        finally {
            this.lock.unlock();
        }
    }

    public void countDown() {
        this.lock.lock();
        try {
            if (--this.state == 0) {
                this.busyCond.signalAll();
            }
        }
        finally {
            this.lock.unlock();
        }
    }

    public void await() throws InterruptedException {
        this.lock.lock();
        try {
            while (this.state > 0) {
                this.busyCond.await();
            }
        }
        finally {
            this.lock.unlock();
        }
    }
}
