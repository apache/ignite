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

public enum Target {

    Username(1), IpAddr(2), ClientId(3), All(4);

    private final int value;

    Target(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static Target valueOf(int value) {
        switch (value) {
        case 1:
            return Username;
        case 2:
            return IpAddr;
        case 3:
            return ClientId;
        case 4:
            return All;
        default:
            return null;
        }
    }

}
