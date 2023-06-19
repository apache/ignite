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

package org.apache.ignite.internal.management.baseline;

/** */
public class BaselineSetCommand extends AbstractBaselineCommand {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Set baseline topology";
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(BaselineCommand.BaselineTaskArg arg) {
        return "Warning: the command will perform changes in baseline.";
    }

    /** {@inheritDoc} */
    @Override public Class<BaselineSetCommandArg> argClass() {
        return BaselineSetCommandArg.class;
    }
}
