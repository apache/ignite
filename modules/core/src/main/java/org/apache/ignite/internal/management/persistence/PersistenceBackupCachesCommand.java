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

package org.apache.ignite.internal.management.persistence;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.management.api.BaseCommand;
import org.apache.ignite.internal.management.api.PositionalArgument;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class PersistenceBackupCachesCommand extends BaseCommand {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @PositionalArgument(example = "cache1,cache2,cache3")
    private List<String> caches;

    /** {@inheritDoc} */
    @Override public String description() {
        return "Backup data files of only given caches";
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        U.writeCollection(out, caches);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(protoVer, in);

        caches = U.readList(in);
    }
}
