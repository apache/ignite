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

package io.stuart.consts;

public interface HttpConst {

    static final String CONTENT_TYPE = "Content-Type";

    static final String CONTENT_LENGTH = "Content-Length";

    static final String APPLICATION_JSON = "application/json";

    static final String CODE = "code";

    static final int UNPROCESSED_CODE = 0;

    static final int PROCESSED_CODE = 1;

    static final String RESULT = "result";

    static final String TOTAL = "total";

    static final String ITEMS = "items";

    static final String CAUSE = "cause";

    static final String LOCATION = "location";

    static final String NOT_LOCAL_AUTH_MODE = "It's not local authentication mode.";

    static final String PARAM_NODE_ID = "paramNodeId";

}
