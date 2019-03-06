/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite.internal.commandline.cache.reset_lost_partitions;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintStream;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Result of CacheResetLostPartitionsTask
 */
public class CacheResetLostPartitionsTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Map group name to result execute message.
     */
    private Map<String, String> msgMap;

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
     * @param out Print stream.
     */
    public void print(PrintStream out) {
        if (msgMap == null || msgMap.isEmpty())
            return;

        for (String message : msgMap.values())
            out.println(message);
    }

    /** */
    public Map<String, String> getMessageMap() {
        return msgMap;
    }

    /** */
    public void setMessageMap(Map<String, String> messageMap) {
        this.msgMap = messageMap;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, msgMap);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        msgMap = U.readMap(in);
    }
}
