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

package org.apache.ignite.internal.processors.bulkload.pipeline;

import java.util.Arrays;
import java.util.List;

/**
 * The PipelineBlock which appends its input to a user-supplied list.
 *
 * <p>The list is set using {@link #output(List)} method.
 */
public class StrListAppenderBlock extends PipelineBlock<String[], Object> {
    /** The output list. */
    private List<List<Object>> output;

    /**
     * Creates the block. List can be configured using {@link #output(List)} method.
     */
    public StrListAppenderBlock() {
        output = null;
    }

    /**
     * Sets the output list.
     *
     * @param output The output list.
     */
    public void output(List<List<Object>> output) {
        this.output = output;
    }

    /** {@inheritDoc} */
    @Override public void accept(String[] elements, boolean isLastPortion) {
        output.add(Arrays.asList(elements));
    }
}
