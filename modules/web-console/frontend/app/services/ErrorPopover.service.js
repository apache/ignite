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

/**
 * Service to show/hide error popover.
 */
export default class ErrorPopover {
    static $inject = ['$popover', '$anchorScroll', '$location', '$timeout', 'IgniteFormUtils'];

    /**
     * @param $popover
     * @param $anchorScroll
     * @param $location
     * @param $timeout
     * @param FormUtils
     */
    constructor($popover, $anchorScroll, $location, $timeout, FormUtils) {
        this.$popover = $popover;
        this.$anchorScroll = $anchorScroll;
        this.$location = $location;
        this.$timeout = $timeout;
        this.FormUtils = FormUtils;

        this.$anchorScroll.yOffset = 55;

        this._popover = null;
    }

    /**
     * Check that element is document area.
     *
     * @param el Element to check.
     * @returns {boolean} True when element in document area.
     */
    static _isElementInViewport(el) {
        const rect = el.getBoundingClientRect();

        return (
            rect.top >= 0 &&
            rect.left >= 0 &&
            rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
            rect.right <= (window.innerWidth || document.documentElement.clientWidth)
        );
    }

    /**
     * Internal show popover message with detected properties.
     *
     * @param id Id element to show popover message.
     * @param message Message to show.
     * @param showTime Time before popover will be hidden.
     */
    _show(id, message, showTime = 5000) {
        const body = $('body');

        let el = body.find('#' + id);

        if (!el || el.length === 0)
            el = body.find('[name="' + id + '"]');

        if (el && el.length > 0) {
            if (!ErrorPopover._isElementInViewport(el[0])) {
                this.$location.hash(el[0].id);

                this.$anchorScroll();
            }

            const newPopover = this.$popover(el, {content: message});

            this._popover = newPopover;

            this.$timeout(() => newPopover.$promise.then(() => {
                newPopover.show();

                // Workaround to fix popover location when content is longer than content template.
                // https://github.com/mgcrea/angular-strap/issues/1497
                this.$timeout(newPopover.$applyPlacement);
            }), 400);
            this.$timeout(() => newPopover.hide(), showTime);
        }
    }

    /**
     * Show popover message.
     *
     * @param {String} id ID of element to show popover.
     * @param {String} message Message to show.
     * @param {Object} [ui] Form UI object. When specified extend section with that name.
     * @param {String} [panelId] ID of element owner panel. When specified focus element with that ID.
     * @param {Number} [showTime] Time before popover will be hidden. 5 sec when not specified.
     * @returns {boolean} False always.
     */
    show(id, message, ui, panelId, showTime) {
        if (this._popover)
            this._popover.hide();

        if (ui) {
            this.FormUtils.ensureActivePanel(ui, panelId, id);

            this.$timeout(() => this._show(id, message, showTime), ui.isPanelLoaded(panelId) ? 200 : 500);
        }
        else
            this._show(id, message);

        return false;
    }

    /**
     * Hide popover message.
     */
    hide() {
        if (this._popover)
            this._popover.hide();
    }
}
