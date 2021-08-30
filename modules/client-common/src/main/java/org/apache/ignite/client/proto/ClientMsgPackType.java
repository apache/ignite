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

package org.apache.ignite.client.proto;

/**
 * Ignite-specific extension type codes.
 */
public class ClientMsgPackType {
    /** Number. */
    public static final byte NUMBER = 1;

    /** Decimal. */
    public static final byte DECIMAL = 2;

    /** UUID. */
    public static final byte UUID = 3;

    /** Date. */
    public static final byte DATE = 4;

    /** Time. */
    public static final byte TIME = 5;

    /** DateTime. */
    public static final byte DATETIME = 6;

    /** DateTime. */
    public static final byte TIMESTAMP = 7;

    /** Bit mask. */
    public static final byte BITMASK = 8;
}
