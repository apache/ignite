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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityCredentials;

/**
 * Client authentication request.
 */
public class GridClientAuthenticationRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Credentials. */
    private SecurityCredentials cred;

    /** User attributes. */
    private Map<String, String> userAttrs;

    /**
     * @return Credentials object.
     */
    public SecurityCredentials credentials() {
        return cred;
    }

    /**
     * @param cred Credentials object.
     */
    public void credentials(SecurityCredentials cred) {
        this.cred = cred;
    }

    /**
     * @return User attributes.
     */
    public Map<String, String> userAttributes() {
        return userAttrs;
    }

    /**
     * @param userAttrs User attributes.
     */
    public void userAttributes(Map<String, String> userAttrs) {
        this.userAttrs = userAttrs;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(cred);

        U.writeMap(out, userAttrs);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        cred = (SecurityCredentials)in.readObject();

        userAttrs = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientAuthenticationRequest.class, this, super.toString());
    }
}
