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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Client authentication request.
 */
public class GridClientAuthenticationRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Credentials. */
    private Object cred;

    /**
     * @return Credentials object.
     */
    public Object credentials() {
        return cred;
    }

    /**
     * @param cred Credentials object.
     */
    public void credentials(Object cred) {
        this.cred = cred;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(cred);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        cred = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientAuthenticationRequest.class, this, super.toString());
    }
}