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

public interface AclConst {

    static final String SUB = "sub";

    static final String PUB = "pub";

    static final String SUBPUB = "subpub";

    static final String DENY = "deny";

    static final String ALLOW = "allow";

    static final String SEPARATOR = "\\|";

    static final String ALL = "$all";

    static final String LOCAL_ADDRESS = "127.0.0.1";

    static final String POUND = "#";

    static final String EQ_PREFIX = "eq(";

    static final String EQ_SUFFIX = ")";

    static final String EQ_POUND = EQ_PREFIX + POUND + EQ_SUFFIX;

    static final String SYS_SLASH_POUND = "$SYS/" + POUND;

}
