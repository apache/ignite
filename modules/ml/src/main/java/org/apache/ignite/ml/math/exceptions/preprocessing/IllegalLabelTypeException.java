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

package org.apache.ignite.ml.math.exceptions.preprocessing;

import org.apache.ignite.IgniteException;

/**
 * Indicates an illegal label type and value.
 */
public class IllegalLabelTypeException extends IgniteException {
    /** */
    private static final long serialVersionUID = 120L;

    /**
     * @param illegalCls The illegal type.
     * @param illegalVal The illegal value.
     * @param desiredCls The desired type.
     */
    public IllegalLabelTypeException(Class illegalCls, Object illegalVal, Class desiredCls) {
        super("The type of label " + illegalCls + " is illegal. The found value is: " + illegalVal + " The type of label should be " + desiredCls);
    }
}
