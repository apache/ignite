/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * TODO: add description.
 */
public class ModelUtils {
    public static void exportModel(Model model, String path, @Nullable IgniteLogger logger){
        ObjectOutputStream outStream = null;
        try {
            outStream = new ObjectOutputStream(new FileOutputStream(path));

            outStream.writeObject(model);
        } catch (IOException e) {
            if (logger != null)
                logger.error("Error opening file", e);
        } finally {
            try {
                if (outStream != null)
                    outStream.close();
            } catch (IOException e) {
                if (logger != null)
                    logger.error("Error closing file", e);
            }
        }
    }

    public static Model importModel(String path, @Nullable IgniteLogger logger){
        Model model = null;
        ObjectInputStream inputStream = null;
        try {
            inputStream = new ObjectInputStream(new FileInputStream(path));
            model = (Model)inputStream.readObject();
        }catch (ClassNotFoundException e) {
            if (logger != null)
                logger.error("Object creation failed.", e);
        } catch (IOException e) {
            if (logger != null)
                logger.error("Error opening file", e);
        } finally {
            try {
                if (inputStream != null)
                    inputStream.close();
            } catch (IOException e) {
                if (logger != null)
                    logger.error("Error closing file", e);
            }
        }
        return model;
    }
}
