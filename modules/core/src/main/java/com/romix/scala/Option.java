/*
 Copyright (C) Roman Levenstein. All Rights Reserved.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package com.romix.scala;

/**
 * Mimic Option in Scala
 *
 * @param <V>
 * @author Roman Levenstein <romixlev@gmail.com>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Option<V> {
    static None none = new None();

    public static <V> Option<V> makeOption(V o) {
        if (o != null)
            return new Some<V>(o);
        else
            return (Option<V>) none;
    }

    public static <V> Option<V> makeOption() {
        return (Option<V>) none;
    }

    public boolean nonEmpty() {
        return false;
    }
}
