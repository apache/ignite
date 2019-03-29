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
package org.apache.ignite.internal.processors.affinity;

/**
 * Interface for historical calculated affinity assignment.
 */
public interface HistoryAffinityAssignment extends AffinityAssignment {
    /**
     * Should return true if instance is "heavy" and should be taken into account during history size management.
     *
     * @return <code>true</code> if adding this instance to history should trigger size check and possible cleanup.
     */
    public boolean requiresHistoryCleanup();

    /**
     * In case this instance is lightweight wrapper of another instance, this method should return reference
     * to an original one. Otherwise, it should return <code>this</code> reference.
     *
     * @return Original instance of <code>this</code> if not applicable.
     */
    public HistoryAffinityAssignment origin();
}
