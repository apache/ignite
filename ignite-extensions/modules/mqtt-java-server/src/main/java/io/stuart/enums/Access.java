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

package io.stuart.enums;

import org.apache.commons.lang3.StringUtils;

import io.stuart.consts.AclConst;

public enum Access {

    Sub(1), Pub(2), SubPub(3);

    private final int value;

    Access(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static Access valueOf(int value) {
        switch (value) {
        case 1:
            return Sub;
        case 2:
            return Pub;
        case 3:
            return SubPub;
        default:
            return null;
        }
    }

    public static Access strOf(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }

        switch (value.toLowerCase()) {
        case AclConst.SUB:
            return Sub;
        case AclConst.PUB:
            return Pub;
        case AclConst.SUBPUB:
            return SubPub;
        default:
            return null;
        }
    }

}
