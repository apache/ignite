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

package io.stuart.services.metrics;

import io.stuart.services.PowerService;
import io.stuart.services.metrics.impl.MetricsServiceImpl;

public interface MetricsService extends PowerService {

    static MetricsService i() {
        return MetricsServiceImpl.getInstance();
    }

    boolean isEnabled();

    void grecord(String clientId, long point, long... values);

    void grecord(long point, long... values);

    void record(String clientId, long... pairs);

    void record(long... pairs);

    long getRetainCount();

    long getRetainMax();

}
