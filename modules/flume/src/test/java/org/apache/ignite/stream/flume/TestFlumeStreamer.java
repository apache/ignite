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

package org.apache.ignite.stream.flume;

import java.util.HashMap;
import java.util.Map;
import org.apache.flume.Event;
import org.apache.ignite.Ignite;

/**
 * A sample data streamer with an event transformation implementation.
 */
public class TestFlumeStreamer extends FlumeStreamer<Event, String, Integer> {

    public void start(Ignite ignite, String cacheName) {
        super.start(ignite, cacheName);
        getDataStreamer().allowOverwrite(true);
        getDataStreamer().autoFlushFrequency(10);
    }

    @Override protected Map<String, Integer> transform(Event event) {
        final Map<String, Integer> map = new HashMap<>();
        String eventStr = new String(event.getBody());
        if (!eventStr.isEmpty()) {
            // expects column-delimited one line
            String[] tokens = eventStr.split(":");
            map.put(tokens[0].trim(), Integer.valueOf(tokens[1].trim()));
        }
        return map;
    }
}
