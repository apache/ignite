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

package org.apache.ignite.internal.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;

/**
 * This exception appears during execution of parallel calculation.
 */
public class ParallelExecutionException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;
    /** List of failed data. */
    private final List<Object> failedDatas = new ArrayList<>();

    /** @param msg Message. */
    public ParallelExecutionException(String msg) {
        super(msg);
    }

    /**
     * @param executionData Data for which execution was failed.
     */
    public void addFailedData(Object executionData) {
        failedDatas.add(executionData);
    }

    /**
     * @return Datas for which execution was failed.
     */
    public List<Object> getFailedDatas() {
        return failedDatas;
    }
}
