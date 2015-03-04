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

package org.apache.ignite.loadtests.streamer;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;

import java.util.*;

/**
 * Configurable streamer load.
 */
public class GridStreamerLoad {
    /** Steamer name. */
    private String name;

    /** Load closures. */
    private List<IgniteInClosure<IgniteStreamer>> clos;

    /**
     * @return Steamer name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Steamer name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Query closure.
     */
    public List<IgniteInClosure<IgniteStreamer>> getClosures() {
        return clos;
    }

    /**
     * @param clos Query closure.
     */
    public void setClosures(List<IgniteInClosure<IgniteStreamer>> clos) {
        this.clos = clos;
    }
}
