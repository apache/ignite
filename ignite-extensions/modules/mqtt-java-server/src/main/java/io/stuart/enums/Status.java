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

public enum Status {

    Stopped(0), Running(1);

    private final int value;

    Status(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static Status valueOf(int value) {
        switch (value) {
        case 0:
            return Stopped;
        case 1:
            return Running;
        default:
            return null;
        }
    }

}
