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
import java.util.function.Function;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class MetricsQuadTuple implements Serializable {

    private static final long serialVersionUID = 499998216453755534L;

    private Object quota;

    private long value;

    private boolean cresc;

    private Function<MetricsQuadTuple, Boolean> func;

    public MetricsQuadTuple() {
        // do nothing...
    }

    public MetricsQuadTuple(Object quota, long value, boolean cresc) {
        this.quota = quota;
        this.value = value;
        this.cresc = cresc;
    }

    public MetricsQuadTuple(Object quota, long value, boolean cresc, Function<MetricsQuadTuple, Boolean> func) {
        this.quota = quota;
        this.value = value;
        this.cresc = cresc;
        this.func = func;
    }

}
