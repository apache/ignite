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

package org.apache.ignite.marshaller.estimate;

/**
 * Exception thrown when something goes wrong during sampling process.
 */
public class SamplingException extends EstimationException {
    /** Serialization ID. */
    private static final long serialVersionUID = 1L;

    /**
     * Create empty exception.
     */
    public SamplingException() {
        // No-op.
    }

    /**
     * @param message Error message.
     */
    public SamplingException(String message) {
        super(message);
    }

    /**
     * @param cause Error cause.
     */
    public SamplingException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message Error message.
     * @param cause Error cause.
     */
    public SamplingException(String message, Throwable cause) {
        super(message, cause);
    }
}
