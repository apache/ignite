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

import org.apache.commons.lang3.StringUtils;

import io.stuart.consts.ParamConst;

public class LogUtil {

    public static String level(String level) {
        if (StringUtils.isBlank(level)) {
            return null;
        }

        // result
        String result = ParamConst.LOG_LEVEL_INFO;
        // to upper case
        String upper = level.toUpperCase();

        switch (upper) {
        case ParamConst.LOG_LEVEL_ALL:
            result = ParamConst.LOG_LEVEL_ALL;
            break;
        case ParamConst.LOG_LEVEL_DEBUG:
            result = ParamConst.LOG_LEVEL_DEBUG;
            break;
        case ParamConst.LOG_LEVEL_ERROR:
            result = ParamConst.LOG_LEVEL_ERROR;
            break;
        case ParamConst.LOG_LEVEL_FATAL:
            result = ParamConst.LOG_LEVEL_FATAL;
            break;
        case ParamConst.LOG_LEVEL_INFO:
            result = ParamConst.LOG_LEVEL_INFO;
            break;
        case ParamConst.LOG_LEVEL_OFF:
            result = ParamConst.LOG_LEVEL_OFF;
            break;
        case ParamConst.LOG_LEVEL_TRACE:
            result = ParamConst.LOG_LEVEL_TRACE;
            break;
        case ParamConst.LOG_LEVEL_WARN:
            result = ParamConst.LOG_LEVEL_WARN;
            break;
        default:
            result = ParamConst.LOG_LEVEL_INFO;
            break;
        }

        return result;
    }

}
