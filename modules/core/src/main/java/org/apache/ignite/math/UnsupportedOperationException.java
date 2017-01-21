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

package org.apache.ignite.math;

import org.apache.ignite.*;

/**
 * Thrown by matrices and vectors to indicate that a specific operation is not
 * supported by the used implementation. In some cases, an operation may be unsupported
 * only in certain cases where, for example, it could not be performed in polynomial time.
 */
public class UnsupportedOperationException extends IgniteException {
    /**
     *
     * @param errMsg Error message.
     */
    public UnsupportedOperationException(String errMsg) {
        super(errMsg);
    }
}
