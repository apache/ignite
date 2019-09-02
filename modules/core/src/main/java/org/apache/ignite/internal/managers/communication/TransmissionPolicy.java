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

package org.apache.ignite.internal.managers.communication;

import java.util.UUID;

/**
 * Class represents ways of data handling for a file ready to be sent through an opened transmission sender session.
 * It is necessary to choose which type of handler will be used and how file should be handled prior to sending file
 * to the remote node.
 *
 * @see GridIoManager.TransmissionSender
 */
public enum TransmissionPolicy {
    /**
     * A file which is considered to be sent through {@link GridIoManager.TransmissionSender}s session will use
     * the {@link TransmissionHandler#fileHandler(UUID, TransmissionMeta)} of {@link TransmissionHandler}
     * to handle transmitted binary data.
     */
    FILE,

    /**
     * A file which is considered to be sent through {@link GridIoManager.TransmissionSender}s session will use
     * the {@link TransmissionHandler#chunkHandler(UUID, TransmissionMeta)} of {@link TransmissionHandler}
     * to handle transmitted binary data. This file will be processed by chunks of handlers defined size.
     */
    CHUNK
}
