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

package org.apache.ignite.internal.client.thin;

/**
 * Server to client notification listener.
 */
interface NotificationListener {
    /**
     * Accept notification.
     *
     * @param ch Client channel which was notified.
     * @param op Client operation.
     * @param rsrcId Resource id.
     * @param payload Notification payload or {@code null} if there is no payload.
     * @param err Error.
     */
    public void acceptNotification(ClientChannel ch, ClientOperation op, long rsrcId, byte[] payload, Exception err);
}
