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

type ActionMenuItem = {icon: string, text: string, click: ng.ICompiledExpression};
type ActionsMenu = ActionMenuItem[];

/**
 * Groups multiple buttons into a single button with all but first buttons in a dropdown
 */
export default class SplitButton {
    actions: ActionsMenu = [];

    static $inject = ['$element'];

    constructor(private $element: JQLite) {}

    $onInit() {
        this.$element[0].classList.add('btn-ignite-group');
    }
}
