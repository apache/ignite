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

package org.apache.ignite.network.processor.internal;

import java.io.Serializable;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.processor.annotations.AutoSerializable;

interface NetworkMessage1 extends NetworkMessage {

}

interface FakeInterface {

}

interface NetworkMessage2 extends NetworkMessage1, Serializable, FakeInterface {

}

@AutoSerializable(messageFactory = TransitiveMessageFactory.class)
public interface TransitiveMessage extends NetworkMessage2 {
    short TYPE = 123;

    int x();

    interface Builder {
        Builder x(int x);

        TransitiveMessage build();
    }
}

class TransitiveMessageImpl implements TransitiveMessage, TransitiveMessage.Builder {
    private int x;

    @Override public int x() {
        return x;
    }

    @Override public short directType() {
        return 0;
    }

    @Override public Builder x(int x) {
        this.x = x;
        return this;
    }

    @Override public TransitiveMessage build() {
        return this;
    }
}
