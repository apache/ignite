/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.entities.metrics;

import java.io.Serializable;
import java.util.concurrent.atomic.LongAdder;

import lombok.Getter;
import lombok.ToString;

@ToString
public class NodeMetrics implements Serializable {

    private static final long serialVersionUID = 3853125081633439170L;

    private static volatile NodeMetrics instance;

    @Getter
    private LongAdder connCount;

    @Getter
    private LongAdder connMax;

    @Getter
    private LongAdder topicCount;

    @Getter
    private LongAdder topicMax;

    @Getter
    private LongAdder sessCount;

    @Getter
    private LongAdder sessMax;

    @Getter
    private LongAdder subCount;

    @Getter
    private LongAdder subMax;

    private NodeMetrics() {
        this.connCount = new LongAdder();
        this.connMax = new LongAdder();
        this.topicCount = new LongAdder();
        this.topicMax = new LongAdder();
        this.sessCount = new LongAdder();
        this.sessMax = new LongAdder();
        this.subCount = new LongAdder();
        this.subMax = new LongAdder();
    }

    public static NodeMetrics getInstance() {
        if (instance == null) {
            synchronized (NodeMetrics.class) {
                if (instance == null) {
                    instance = new NodeMetrics();
                }
            }
        }

        return instance;
    }

}
