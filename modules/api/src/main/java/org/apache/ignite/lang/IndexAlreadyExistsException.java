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

package org.apache.ignite.lang;

/**
 * This exception is thrown when a new index failed to be created, because another index with the same name already exists.
 */
public class IndexAlreadyExistsException extends IgniteException {
    /**
     * Create a new exception with given index name.
     *
     * @param name Index name.
     */
    public IndexAlreadyExistsException(String name) {
        super(LoggerMessageHelper.format("Index already exists [name={}]", name));
    }
}
