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

import java.io.File;

import io.stuart.exceptions.StartException;

public class DirUtil {

    public static boolean mkdirs(String path) {
        File file = new File(path);

        if (file.exists()) {
            return false;
        }

        try {
            return file.mkdirs();
        } catch (SecurityException e) {
            throw new StartException(e);
        }
    }

}
