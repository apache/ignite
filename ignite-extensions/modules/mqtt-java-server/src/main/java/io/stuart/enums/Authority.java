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

public enum Authority {

    Deny(0), Allow(1);

    private final int value;

    Authority(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static Authority valueOf(int value) {
        switch (value) {
        case 0:
            return Deny;
        case 1:
            return Allow;
        default:
            return null;
        }
    }

    public static Authority strOf(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }

        switch (value.toLowerCase()) {
        case AclConst.DENY:
            return Deny;
        case AclConst.ALLOW:
            return Allow;
        default:
            return null;
        }
    }

}
