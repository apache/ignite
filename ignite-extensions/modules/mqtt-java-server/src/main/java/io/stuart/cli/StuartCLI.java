/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.cli;

import io.vertx.core.cli.CLI;
import io.vertx.core.cli.UsageMessageFormatter;
import io.vertx.core.cli.impl.DefaultCLI;

public class StuartCLI extends DefaultCLI {

    private static final int USAGE_WIDTH = 120;

    @Override
    public CLI usage(StringBuilder builder) {
        UsageMessageFormatter formatter = new UsageMessageFormatter();
        formatter.setWidth(USAGE_WIDTH);
        formatter.usage(builder, this);
        return this;
    }

    @Override
    public CLI usage(StringBuilder builder, String prefix) {
        UsageMessageFormatter formatter = new UsageMessageFormatter();
        formatter.setWidth(USAGE_WIDTH);
        formatter.usage(builder, prefix, this);
        return this;
    }

}
