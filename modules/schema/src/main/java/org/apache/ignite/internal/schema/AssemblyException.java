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

package org.apache.ignite.internal.schema;

/**
 * The exception is thrown when the row assembler encountered an unrecoverable error during the field encoding.
 * After the exception is thrown, the assembler remains in an invalid state and should be discarded.
 */
public class AssemblyException extends RuntimeException {
    /**
     * @param errMsg Error message
     * @param cause Cause for this error.
     */
    public AssemblyException(String errMsg, Exception cause) {
        super(errMsg, cause);
    }
}
