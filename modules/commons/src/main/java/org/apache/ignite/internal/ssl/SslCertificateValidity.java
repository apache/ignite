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

package org.apache.ignite.internal.ssl;

/** The interval in which every certificate in a key's chain is valid. */
public record SslCertificateValidity(long notBefore, long notAfter) {
    /**
     * @param time Current time in milliseconds since the epoch.
     * @return Whether the chain is valid at the given time.
     */
    public boolean validAt(long time) {
        return notBefore <= time && time <= notAfter;
    }
}
