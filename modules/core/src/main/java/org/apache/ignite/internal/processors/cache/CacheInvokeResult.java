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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.UnregisteredBinaryTypeException;
import org.apache.ignite.internal.UnregisteredClassException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Implementation of {@link EntryProcessorResult}.
 */
public class CacheInvokeResult<T> implements EntryProcessorResult<T>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude(sensitive = true)
    private T res;

    /** */
    private Exception err;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public CacheInvokeResult() {
        // No-op.
    }

    /**
     * Static constructor.
     *
     * @param res Computed result.
     * @return New instance.
     */
    public static <T> CacheInvokeResult<T> fromResult(T res) {
        CacheInvokeResult<T> cacheRes = new CacheInvokeResult<>();

        cacheRes.res = res;

        return cacheRes;
    }

    /**
     * @return Result.
     */
    public T result() {
        return res;
    }

    /**
     * Entry processor error;
     */
    public Exception error() {
        return err;
    }

    /**
     * Static constructor.
     *
     * @param err Exception thrown by {@link EntryProcessor#process(MutableEntry, Object...)}.
     * @return New instance.
     */
    public static <T> CacheInvokeResult<T> fromError(Exception err) {
        assert err != null;

        CacheInvokeResult<T> res = new CacheInvokeResult<>();

        res.err = err;

        return res;
    }

    /** {@inheritDoc} */
    @Override public T get() throws EntryProcessorException {
        if (err != null) {
            if (err instanceof UnregisteredClassException || err instanceof UnregisteredBinaryTypeException)
                throw (IgniteException) err;

            if (err instanceof EntryProcessorException)
                throw (EntryProcessorException)err;

            throw new EntryProcessorException(err);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(res);

        out.writeObject(err);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        res = (T)in.readObject();

        err = (Exception)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheInvokeResult.class, this);
    }
}