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
package org.apache.ignite.internal.binary.cheap;

/**
 * Pointer in array where UTF-8 encoded bytes are stored.
 * We want to use this "String" whenever result of read will be sent over the wire.
 * If we're serving local user request regular {@link String} must be created on read.
 */
public class CheapString {
    /** */
    public final byte[] arr;

    /** */
    public final int off;

    /** */
    public final int len;

    /** */
    public CheapString(byte[] arr) {
        this(arr, 0, arr.length);
    }

    /** */
    public CheapString(byte[] arr, int off, int len) {
        this.arr = arr;
        this.off = off;
        this.len = len;
    }
}
