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

package org.apache.ignite.client;

/**
 * This interface provides a list of addresses of Ignite server nodes within a cluster. Thin client uses the list to
 * route user requests. There are cases when the list is not static, for example in cloud environment. In such cases
 * addresses of nodes and/or number of server nodes can change. Implementation of this interface should handle these.
 */
public interface IgniteClientAddressFinder {
    /**
     * Get addresses of Ignite server nodes within a cluster. An address can be IPv4 address or hostname, with or
     * without port. If port is not set then Ignite will generate multiple addresses for default port range.
     *
     * @return Addresses of Ignite server nodes within a cluster.
     */
    public String[] getAddresses();
}
