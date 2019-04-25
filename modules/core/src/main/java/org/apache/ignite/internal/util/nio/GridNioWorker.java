/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.nio;

import java.util.Collection;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
interface GridNioWorker {
    /**
     * @param req Change request.
     */
    public void offer(GridNioServer.SessionChangeRequest req);

    /**
     * @param reqs Change requests.
     */
    public void offer(Collection<GridNioServer.SessionChangeRequest> reqs);

    /**
     * @param ses Session.
     * @return Session state change requests.
     */
    @Nullable public List<GridNioServer.SessionChangeRequest> clearSessionRequests(GridNioSession ses);

    /**
     * @param ses Session to register write interest for.
     */
    public void registerWrite(GridSelectorNioSessionImpl ses);
}
