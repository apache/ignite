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

package org.apache.ignite.internal.network.processor;

import javax.lang.model.element.Element;
import org.jetbrains.annotations.Nullable;

/**
 * Exception used by all annotation-processor related classes to gracefully display processing errors.
 */
public class ProcessingException extends RuntimeException {
    /**
     *
     */
    @Nullable
    private final Element element;

    /**
     * @param message message
     */
    public ProcessingException(String message) {
        this(message, null, null);
    }

    /**
     * @param message message
     * @param cause   cause
     */
    public ProcessingException(String message, @Nullable Throwable cause) {
        this(message, cause, null);
    }

    /**
     * @param message message
     * @param cause   cause
     * @param element element which processing triggered the exception
     */
    public ProcessingException(String message, @Nullable Throwable cause, @Nullable Element element) {
        super(message, cause);

        this.element = element;
    }

    /**
     * @return element which processing triggered the exception
     */
    @Nullable
    public Element getElement() {
        return element;
    }
}
