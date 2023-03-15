/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.builder;

/**
 * {@code RuntimeException} to be thrown when there are builder-related errors.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BuilderException extends RuntimeException {

    private static final long serialVersionUID = 345236345764574567L;

    /**
     * Constructs a new index exception with the specified formatted detail message.
     *
     * @param cause the cause, which is saved for later retrieval by the {@link #getCause()} method
     * @param message the detail message, which is saved for later retrieval by the {@link #getMessage()} method
     * @param args arguments referenced by the format specifiers in the format message
     */
    public BuilderException(Throwable cause, String message, Object... args) {
        super(String.format(message, args), cause);
    }

}
