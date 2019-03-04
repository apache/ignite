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

export default class PanelCollapsible {
    /** @type {Boolean} */
    opened;
    /** @type {ng.ICompiledExpression} */
    onOpen;
    /** @type {ng.ICompiledExpression} */
    onClose;
    /** @type {String} */
    disabled;

    static $inject = ['$transclude'];

    /**
     * @param {ng.ITranscludeFunction} $transclude
     */
    constructor($transclude) {
        this.$transclude = $transclude;
    }

    toggle() {
        if (this.opened)
            this.close();
        else
            this.open();
    }

    open() {
        if (this.disabled)
            return;

        this.opened = true;

        if (this.onOpen && this.opened)
            this.onOpen({});
    }

    close() {
        if (this.disabled)
            return;

        this.opened = false;

        if (this.onClose && !this.opened)
            this.onClose({});
    }
}
