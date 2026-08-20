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

package org.apache.ignite.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.plugin.extensions.communication.Message;

public class TestMarshalledArrayMapMessage implements Message {
    @Marshalled(keys = "mapKeys", values = "mapVals")
    Map<GridTopicMessage, List<GridTopicMessage>> theMap;

    @Marshalled(keys = "fixedMapKeys", values = "fixedMapVals")
    final Map<GridTopicMessage, List<GridTopicMessage>> fixedMap = new HashMap<>();

    @Order(0)
    GridTopicMessage[] mapKeys;

    @Order(1)
    List<GridTopicMessage>[] mapVals;

    @Order(2)
    GridTopicMessage[] fixedMapKeys;

    @Order(3)
    List<GridTopicMessage>[] fixedMapVals;

    public short directType() {
        return 0;
    }
}
