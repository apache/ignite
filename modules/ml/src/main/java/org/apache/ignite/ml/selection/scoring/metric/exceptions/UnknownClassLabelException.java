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

package org.apache.ignite.ml.selection.scoring.metric.exceptions;

import org.apache.ignite.IgniteException;

/**
 * Indicates an unknown class label for metric calculator.
 */
public class UnknownClassLabelException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;


    /**
     * @param incorrectVal Incorrect value.
     * @param positiveClsLb Positive class label.
     * @param negativeClsLb Negative class label.
     */
    public UnknownClassLabelException(double incorrectVal, double positiveClsLb, double negativeClsLb) {
        super("The next class label: " + incorrectVal + " is not positive class label: " + positiveClsLb + " or negative class label: " + negativeClsLb);
    }
}
