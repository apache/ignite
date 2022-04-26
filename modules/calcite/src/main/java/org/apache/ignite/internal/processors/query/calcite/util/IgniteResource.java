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

package org.apache.ignite.internal.processors.query.calcite.util;

import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.validate.SqlValidatorException;

/**
 *
 */
public interface IgniteResource {
    /** */
    IgniteResource INSTANCE = Resources.create(IgniteResource.class);

    /** */
    @Resources.BaseMessage("Illegal alias. {0} is reserved name.")
    Resources.ExInst<SqlValidatorException> illegalAlias(String a0);

    /** */
    @Resources.BaseMessage("Cannot update field \"{0}\". You cannot update key, key fields or val field in case the val is a complex type.")
    Resources.ExInst<SqlValidatorException> cannotUpdateField(String field);

    /** */
    @Resources.BaseMessage("Illegal aggregate function. {0} is unsupported at the moment.")
    Resources.ExInst<SqlValidatorException> unsupportedAggregationFunction(String a0);

    /** */
    @Resources.BaseMessage("Illegal value of {0}. The value must be positive and less than Integer.MAX_VALUE " +
        "(" + Integer.MAX_VALUE + ")." )
    Resources.ExInst<SqlValidatorException> correctIntegerLimit(String a0);

    /** */
    @Resources.BaseMessage("Option ''{0}'' has already been defined")
    Resources.ExInst<SqlValidatorException> optionAlreadyDefined(String optName);

    /** */
    @Resources.BaseMessage("Illegal value ''{0}''. The value must be UUID")
    Resources.ExInst<SqlValidatorException> illegalUuid(String value);

    /** */
    @Resources.BaseMessage("Illegal value ''{0}''. The value must be IgniteUuid")
    Resources.ExInst<SqlValidatorException> illegalIgniteUuid(String value);

    /** */
    @Resources.BaseMessage("Illegal value ''{0}''. The value should have format '{node_id}_{query_id}', " +
        "e.g. '6fa749ee-7cf8-4635-be10-36a1c75267a7_54321'")
    Resources.ExInst<SqlValidatorException> illegalGlobalQueryId(String value);

    /** */
    @Resources.BaseMessage("Cannot parse pair:''{0}''. The pair should have format 'key=value'")
    Resources.ExInst<SqlValidatorException> cannotParsePair(String value);

    /** */
    @Resources.BaseMessage("Illegal option ''{0}''")
    Resources.ExInst<SqlValidatorException> illegalOption(String value);

    /** */
    @Resources.BaseMessage("Modify operation is not supported for table ''{0}''")
    Resources.ExInst<SqlValidatorException> modifyTableNotSupported(String table);
}
