package org.apache.ignite.internal.processors.query.stat.hll;

/*
 * Copyright 2013 Aggregate Knowledge, Inc.
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

/**
 * The types of algorithm/data structure that {@link HLL} can utilize. For more
 * information, see the Javadoc for {@link HLL}.
 */
public enum HLLType {
    EMPTY,
    EXPLICIT,
    SPARSE,
    FULL,
    UNDEFINED/*used by the PostgreSQL implementation to indicate legacy/corrupt/incompatible/unknown formats*/;
}
