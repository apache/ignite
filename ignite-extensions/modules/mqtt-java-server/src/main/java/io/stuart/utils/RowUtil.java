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

package io.stuart.utils;

import java.util.UUID;

public class RowUtil {

    public static UUID getUUID(Object obj) {
        if (obj == null) {
            return null;
        }

        return IdUtil.uuid(obj.toString());
    }

    public static String getStr(Object obj) {
        if (obj == null) {
            return null;
        }

        return obj.toString();
    }

    public static boolean getBool(Object obj) {
        if (obj == null) {
            return false;
        }

        return Boolean.parseBoolean(obj.toString());
    }

    public static int getInt(Object obj) {
        if (obj == null) {
            return 0;
        }

        return Integer.parseInt(obj.toString());
    }

    public static long getLong(Object obj) {
        if (obj == null) {
            return 0L;
        }

        return Long.parseLong(obj.toString());
    }

}
