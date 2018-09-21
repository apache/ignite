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
import _ from 'lodash';

export default ['IgniteFormUtils', ['$window', 'IgniteFocus', '$rootScope', function($window, Focus, $rootScope) {
    function ensureActivePanel(ui, pnl, focusId) {
        if (ui && ui.loadPanel) {
            const collapses = $('[bs-collapse-target]');

            ui.loadPanel(pnl);

            const idx = _.findIndex(collapses, function(collapse) {
                return collapse.id === pnl;
            });

            if (idx >= 0) {
                const activePanels = ui.activePanels;

                if (!_.includes(ui.topPanels, idx)) {
                    ui.expanded = true;

                    const customExpanded = ui[pnl];

                    if (customExpanded)
                        ui[customExpanded] = true;
                }

                if (!activePanels || activePanels.length < 1)
                    ui.activePanels = [idx];
                else if (!_.includes(activePanels, idx)) {
                    const newActivePanels = _.cloneDeep(activePanels);

                    newActivePanels.push(idx);

                    ui.activePanels = newActivePanels;
                }
            }

            if (!_.isNil(focusId))
                Focus.move(focusId);
        }
    }

    let context = null;

    /**
     * Calculate width of specified text in body's font.
     *
     * @param text Text to calculate width.
     * @returns {Number} Width of text in pixels.
     */
    function measureText(text) {
        if (!context) {
            const canvas = document.createElement('canvas');

            context = canvas.getContext('2d');

            const style = window.getComputedStyle(document.getElementsByTagName('body')[0]);

            context.font = style.fontSize + ' ' + style.fontFamily;
        }

        return context.measureText(text).width;
    }

    /**
     * Compact java full class name by max number of characters.
     *
     * @param names Array of class names to compact.
     * @param nameLength Max available width in characters for simple name.
     * @returns {*} Array of compacted class names.
     */
    function compactByMaxCharts(names, nameLength) {
        for (let nameIx = 0; nameIx < names.length; nameIx++) {
            const s = names[nameIx];

            if (s.length > nameLength) {
                let totalLength = s.length;

                const packages = s.split('.');

                const packageCnt = packages.length - 1;

                for (let i = 0; i < packageCnt && totalLength > nameLength; i++) {
                    if (packages[i].length > 0) {
                        totalLength -= packages[i].length - 1;

                        packages[i] = packages[i][0];
                    }
                }

                if (totalLength > nameLength) {
                    const className = packages[packageCnt];

                    const classNameLen = className.length;

                    let remains = Math.min(nameLength - totalLength + classNameLen, classNameLen);

                    if (remains < 3)
                        remains = Math.min(3, classNameLen);

                    packages[packageCnt] = className.substring(0, remains) + '...';
                }

                let result = packages[0];

                for (let i = 1; i < packages.length; i++)
                    result += '.' + packages[i];

                names[nameIx] = result;
            }
        }

        return names;
    }

    /**
     * Compact java full class name by max number of pixels.
     *
     * @param names Array of class names to compact.
     * @param nameLength Max available width in characters for simple name. Used for calculation optimization.
     * @param nameWidth Maximum available width in pixels for simple name.
     * @returns {*} Array of compacted class names.
     */
    function compactByMaxPixels(names, nameLength, nameWidth) {
        if (nameWidth <= 0)
            return names;

        const fitted = [];

        const widthByName = [];

        const len = names.length;

        let divideTo = len;

        for (let nameIx = 0; nameIx < len; nameIx++) {
            fitted[nameIx] = false;

            widthByName[nameIx] = nameWidth;
        }

        // Try to distribute space from short class names to long class names.
        let remains = 0;

        do {
            for (let nameIx = 0; nameIx < len; nameIx++) {
                if (!fitted[nameIx]) {
                    const curNameWidth = measureText(names[nameIx]);

                    if (widthByName[nameIx] > curNameWidth) {
                        fitted[nameIx] = true;

                        remains += widthByName[nameIx] - curNameWidth;

                        divideTo -= 1;

                        widthByName[nameIx] = curNameWidth;
                    }
                }
            }

            const remainsByName = remains / divideTo;

            for (let nameIx = 0; nameIx < len; nameIx++) {
                if (!fitted[nameIx])
                    widthByName[nameIx] += remainsByName;
            }
        }
        while (remains > 0);

        // Compact class names to available for each space.
        for (let nameIx = 0; nameIx < len; nameIx++) {
            const s = names[nameIx];

            if (s.length > (nameLength / 2 | 0)) {
                let totalWidth = measureText(s);

                if (totalWidth > widthByName[nameIx]) {
                    const packages = s.split('.');

                    const packageCnt = packages.length - 1;

                    for (let i = 0; i < packageCnt && totalWidth > widthByName[nameIx]; i++) {
                        if (packages[i].length > 1) {
                            totalWidth -= measureText(packages[i].substring(1, packages[i].length));

                            packages[i] = packages[i][0];
                        }
                    }

                    let shortPackage = '';

                    for (let i = 0; i < packageCnt; i++)
                        shortPackage += packages[i] + '.';

                    const className = packages[packageCnt];

                    const classLen = className.length;

                    let minLen = Math.min(classLen, 3);

                    totalWidth = measureText(shortPackage + className);

                    // Compact class name if shorten package path is very long.
                    if (totalWidth > widthByName[nameIx]) {
                        let maxLen = classLen;
                        let middleLen = (minLen + (maxLen - minLen) / 2 ) | 0;

                        while (middleLen !== minLen && middleLen !== maxLen) {
                            const middleLenPx = measureText(shortPackage + className.substr(0, middleLen) + '...');

                            if (middleLenPx > widthByName[nameIx])
                                maxLen = middleLen;
                            else
                                minLen = middleLen;

                            middleLen = (minLen + (maxLen - minLen) / 2 ) | 0;
                        }

                        names[nameIx] = shortPackage + className.substring(0, middleLen) + '...';
                    }
                    else
                        names[nameIx] = shortPackage + className;
                }
            }
        }

        return names;
    }

    /**
     * Compact any string by max number of pixels.
     *
     * @param label String to compact.
     * @param nameWidth Maximum available width in pixels for simple name.
     * @returns {*} Compacted string.
     */
    function compactLabelByPixels(label, nameWidth) {
        if (nameWidth <= 0)
            return label;

        const totalWidth = measureText(label);

        if (totalWidth > nameWidth) {
            let maxLen = label.length;
            let minLen = Math.min(maxLen, 3);
            let middleLen = (minLen + (maxLen - minLen) / 2 ) | 0;

            while (middleLen !== minLen && middleLen !== maxLen) {
                const middleLenPx = measureText(label.substr(0, middleLen) + '...');

                if (middleLenPx > nameWidth)
                    maxLen = middleLen;
                else
                    minLen = middleLen;

                middleLen = (minLen + (maxLen - minLen) / 2 ) | 0;
            }

            return label.substring(0, middleLen) + '...';
        }

        return label;
    }

    /**
     * Calculate available width for text in link to edit element.
     *
     * @param index Showed index of element for calculation of maximum width in pixels.
     * @param id Id of contains link table.
     * @returns {*[]} First element is length of class for single value, second element is length for pair vlaue.
     */
    function availableWidth(index, id) {
        const idElem = $('#' + id);

        let width = 0;

        switch (idElem.prop('tagName')) {
            // Detection of available width in presentation table row.
            case 'TABLE':
                const cont = $(idElem.find('tr')[index - 1]).find('td')[0];

                width = cont.clientWidth;

                if (width > 0) {
                    const children = $(cont).children(':not("a")');

                    _.forEach(children, function(child) {
                        if ('offsetWidth' in child)
                            width -= $(child).outerWidth(true);
                    });
                }

                break;

            // Detection of available width in dropdown row.
            case 'A':
                width = idElem.width();

                $(idElem).children(':not("span")').each(function(ix, child) {
                    if ('offsetWidth' in child)
                        width -= child.offsetWidth;
                });

                break;

            default:
        }

        return width | 0;
    }

    // TODO: move somewhere else
    function triggerValidation(form) {
        const fe = (m) => Object.keys(m.$error)[0];
        const em = (e) => (m) => {
            if (!e)
                return;

            const walk = (m) => {
                if (!m.$error[e])
                    return;

                if (m.$error[e] === true)
                    return m;

                return walk(m.$error[e][0]);
            };

            return walk(m);
        };

        $rootScope.$broadcast('$showValidationError', em(fe(form))(form));
    }

    return {
        /**
         * Cut class name by width in pixel or width in symbol count.
         *
         * @param id Id of parent table.
         * @param index Row number in table.
         * @param maxLength Maximum length in symbols for all names.
         * @param names Array of class names to compact.
         * @param divider String to visualy divide items.
         * @returns {*} Array of compacted class names.
         */
        compactJavaName(id, index, maxLength, names, divider) {
            divider = ' ' + divider + ' ';

            const prefix = index + ') ';

            const nameCnt = names.length;

            const nameLength = ((maxLength - 3 * (nameCnt - 1)) / nameCnt) | 0;

            try {
                const nameWidth = (availableWidth(index, id) - measureText(prefix) - (nameCnt - 1) * measureText(divider)) /
                    nameCnt | 0;

                // HTML5 calculation of showed message width.
                names = compactByMaxPixels(names, nameLength, nameWidth);
            }
            catch (err) {
                names = compactByMaxCharts(names, nameLength);
            }

            let result = prefix + names[0];

            for (let nameIx = 1; nameIx < names.length; nameIx++)
                result += divider + names[nameIx];

            return result;
        },
        /**
         * Compact text by width in pixels or symbols count.
         *
         * @param id Id of parent table.
         * @param index Row number in table.
         * @param maxLength Maximum length in symbols for all names.
         * @param label Text to compact.
         * @returns Compacted label text.
         */
        compactTableLabel(id, index, maxLength, label) {
            label = index + ') ' + label;

            try {
                const nameWidth = availableWidth(index, id) | 0;

                // HTML5 calculation of showed message width.
                label = compactLabelByPixels(label, nameWidth);
            }
            catch (err) {
                const nameLength = maxLength - 3 | 0;

                label = label.length > maxLength ? label.substr(0, nameLength) + '...' : label;
            }

            return label;
        },
        widthIsSufficient(id, index, text) {
            try {
                const available = availableWidth(index, id);

                const required = measureText(text);

                return !available || available >= Math.floor(required);
            }
            catch (err) {
                return true;
            }
        },
        ensureActivePanel(panels, id, focusId) {
            ensureActivePanel(panels, id, focusId);
        },
        saveBtnTipText(dirty, objectName) {
            if (dirty)
                return 'Save ' + objectName;

            return 'Nothing to save';
        },
        formUI() {
            return {
                ready: false,
                expanded: false,
                loadedPanels: [],
                loadPanel(pnl) {
                    if (!_.includes(this.loadedPanels, pnl))
                        this.loadedPanels.push(pnl);
                },
                isPanelLoaded(pnl) {
                    return _.includes(this.loadedPanels, pnl);
                }
            };
        },
        markPristineInvalidAsDirty(ngModelCtrl) {
            if (ngModelCtrl && ngModelCtrl.$invalid && ngModelCtrl.$pristine)
                ngModelCtrl.$setDirty();
        },
        triggerValidation
    };
}]];
