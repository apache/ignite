/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
