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
package org.apache.ignite.internal.commandline.cache.reset_lost_partitions;

import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/**
 * Result of CacheResetLostPartitionsTask
 */
public class CacheResetLostPartitionsTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Map group name to result execute message.
     */
    Map<String, String> msgMap;

    /**
     * @param groupName - Cache group name.
     * @param message - Job result message.
     * @return the previous value associated with <tt>key</tt>, or <tt>null</tt>
     */
    public String put(String groupName, String message) {
        return this.msgMap.put(groupName, message);
    }

    /**
     * Print job result.
     *
     * @param printer Printer.
     */
    public void print(Consumer<String> printer) {
        if (msgMap == null || msgMap.isEmpty())
            return;

        for (String msg : msgMap.values())
            printer.accept(msg);
    }

    /** */
    public Map<String, String> getMessageMap() {
        return msgMap;
    }

    /** */
    public void setMessageMap(Map<String, String> messageMap) {
        this.msgMap = messageMap;
    }
}
