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

package org.apache.ignite.console.agent.db.dialect;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * DB2 specific metadata dialect.
 */
public class DB2MetadataDialect extends JdbcMetadataDialect {
    /** {@inheritDoc} */
    @Override public Set<String> systemSchemas() {
        return new HashSet<>(Arrays.asList("SYSIBM", "SYSCAT", "SYSSTAT", "SYSTOOLS", "SYSFUN", "SYSIBMADM",
            "SYSIBMINTERNAL", "SYSIBMTS", "SYSPROC", "SYSPUBLIC"));
    }
}
