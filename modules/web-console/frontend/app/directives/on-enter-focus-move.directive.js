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

// Directive to move focus to specified element on ENTER key.
export default ['igniteOnEnterFocusMove', ['IgniteFocus', function(Focus) {
    return function(scope, elem, attrs) {
        elem.on('keydown keypress', (event) => {
            if (event.which === 13) {
                event.preventDefault();

                Focus.move(attrs.igniteOnEnterFocusMove);
            }
        });
    };
}]];
