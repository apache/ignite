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

const consoleModule = angular.module('ignite-console.legacy',
    [
        'smart-table',
        'treeControl',
        'ui.grid',
        'ui.grid.saveState',
        'ui.grid.selection',
        'ui.grid.resizeColumns',
        'ui.grid.autoResize',
        'ui.grid.exporter',
        'nvd3',
        'dndLists'
        /* ignite:plugins */
        /* endignite */
    ]);

import alertTemplate from '../views/templates/alert.jade!';

consoleModule.run(['$rootScope', '$http', '$state', '$common', 'Auth', 'User', 'gettingStarted',
    ($root, $http, $state, $common, Auth, User, gettingStarted) => {
        $root.gettingStarted = gettingStarted;

        $root.revertIdentity = function() {
            $http
                .get('/api/v1/admin/revert/identity')
                .then(User.read)
                .then(function(user) {
                    $root.$broadcast('user', user);

                    $state.go('settings.admin');
                })
                .catch(function(errMsg) {
                    $common.showError($common.errorMessage(errMsg));
                });
        };
    }
]);

// Modal popup configuration.
consoleModule.config(['$modalProvider', ($modalProvider) => {
    angular.extend($modalProvider.defaults, {
        html: true
    });
}]);

// Comboboxes configuration.
consoleModule.config(['$popoverProvider', ($popoverProvider) => {
    angular.extend($popoverProvider.defaults, {
        trigger: 'manual',
        placement: 'right',
        container: 'body',
        templateUrl: '/templates/validation-error.html'
    });
}]);

// Tooltips configuration.
consoleModule.config(['$tooltipProvider', ($tooltipProvider) => {
    angular.extend($tooltipProvider.defaults, {
        container: 'body',
        delay: 150,
        placement: 'right',
        html: 'true',
        trigger: 'click hover'
    });
}]);

// Comboboxes configuration.
consoleModule.config(['$selectProvider', ($selectProvider) => {
    angular.extend($selectProvider.defaults, {
        container: 'body',
        maxLength: '5',
        allText: 'Select All',
        noneText: 'Clear All',
        templateUrl: '/templates/select.html',
        iconCheckmark: 'fa fa-check',
        caretHtml: ''
    });
}]);

// Alerts configuration.
consoleModule.config(['$alertProvider', ($alertProvider) => {
    angular.extend($alertProvider.defaults, {
        container: 'body',
        placement: 'top-right',
        duration: '5',
        template: alertTemplate(),
        type: 'danger'
    });
}]);

// Modals configuration.
consoleModule.config(['$modalProvider', ($modalProvider) => {
    angular.extend($modalProvider.defaults, {
        animation: 'am-fade-and-scale'
    });
}]);

// Dropdowns configuration.
consoleModule.config(['$dropdownProvider', ($dropdownProvider) => {
    angular.extend($dropdownProvider.defaults, {
        templateUrl: 'templates/dropdown.html'
    });
}]);

// Common functions to be used in controllers.
consoleModule.service('$common', ['$alert', '$popover', '$anchorScroll', '$location', '$timeout', '$focus', '$window',
    ($alert, $popover, $anchorScroll, $location, $timeout, $focus, $window) => {
        $anchorScroll.yOffset = 55;

        function isDefined(v) {
            return !_.isNil(v);
        }

        function isEmptyString(s) {
            if (isDefined(s))
                return s.trim().length === 0;

            return true;
        }

        let msgModal;

        function errorMessage(errMsg) {
            if (errMsg) {
                if (errMsg.hasOwnProperty('message'))
                    return errMsg.message;

                return errMsg;
            }

            return 'Internal server error.';
        }

        function showError(msg, placement, container, persistent, icon) {
            if (msgModal)
                msgModal.hide();

            msgModal = $alert({
                title: errorMessage(msg),
                placement: placement ? placement : 'top-right',
                container: container ? container : 'body',
                duration: persistent ? false : 5
            });

            msgModal.$scope.icon = icon ? icon : 'fa-exclamation-triangle';

            return false;
        }

        const javaBuiltInClasses = [
            'BigDecimal', 'Boolean', 'Byte', 'Date', 'Double', 'Float', 'Integer', 'Long', 'Object', 'Short', 'String', 'Time', 'Timestamp', 'UUID'
        ];

        const javaBuiltInTypes = [
            'BigDecimal', 'boolean', 'Boolean', 'byte', 'Byte', 'Date', 'double', 'Double', 'float', 'Float',
            'int', 'Integer', 'long', 'Long', 'Object', 'short', 'Short', 'String', 'Time', 'Timestamp', 'UUID'
        ];

        const javaBuiltInFullNameClasses = [
            'java.math.BigDecimal', 'java.lang.Boolean', 'java.lang.Byte', 'java.sql.Date', 'java.lang.Double',
            'java.lang.Float', 'java.lang.Integer', 'java.lang.Long', 'java.lang.Object', 'java.lang.Short',
            'java.lang.String', 'java.sql.Time', 'java.sql.Timestamp', 'java.util.UUID'
        ];

        /**
         * @param clsName Class name to check.
         * @returns {Boolean} 'true' if given class name is a java build-in type.
         */
        function isJavaBuiltInClass(clsName) {
            if (isEmptyString(clsName))
                return false;

            return _.includes(javaBuiltInClasses, clsName) || _.includes(javaBuiltInFullNameClasses, clsName);
        }

        const SUPPORTED_JDBC_TYPES = [
            'BIGINT',
            'BIT',
            'BOOLEAN',
            'BLOB',
            'CHAR',
            'CLOB',
            'DATE',
            'DECIMAL',
            'DOUBLE',
            'FLOAT',
            'INTEGER',
            'LONGNVARCHAR',
            'LONGVARCHAR',
            'NCHAR',
            'NUMERIC',
            'NVARCHAR',
            'REAL',
            'SMALLINT',
            'TIME',
            'TIMESTAMP',
            'TINYINT',
            'VARCHAR'
        ];

        const ALL_JDBC_TYPES = [
            {dbName: 'BIT', dbType: -7, javaType: 'Boolean', primitiveType: 'boolean'},
            {dbName: 'TINYINT', dbType: -6, javaType: 'Byte', primitiveType: 'byte'},
            {dbName: 'SMALLINT', dbType: 5, javaType: 'Short', primitiveType: 'short'},
            {dbName: 'INTEGER', dbType: 4, javaType: 'Integer', primitiveType: 'int'},
            {dbName: 'BIGINT', dbType: -5, javaType: 'Long', primitiveType: 'long'},
            {dbName: 'FLOAT', dbType: 6, javaType: 'Float', primitiveType: 'float'},
            {dbName: 'REAL', dbType: 7, javaType: 'Double', primitiveType: 'double'},
            {dbName: 'DOUBLE', dbType: 8, javaType: 'Double', primitiveType: 'double'},
            {dbName: 'NUMERIC', dbType: 2, javaType: 'BigDecimal'},
            {dbName: 'DECIMAL', dbType: 3, javaType: 'BigDecimal'},
            {dbName: 'CHAR', dbType: 1, javaType: 'String'},
            {dbName: 'VARCHAR', dbType: 12, javaType: 'String'},
            {dbName: 'LONGVARCHAR', dbType: -1, javaType: 'String'},
            {dbName: 'DATE', dbType: 91, javaType: 'Date'},
            {dbName: 'TIME', dbType: 92, javaType: 'Time'},
            {dbName: 'TIMESTAMP', dbType: 93, javaType: 'Timestamp'},
            {dbName: 'BINARY', dbType: -2, javaType: 'Object'},
            {dbName: 'VARBINARY', dbType: -3, javaType: 'Object'},
            {dbName: 'LONGVARBINARY', dbType: -4, javaType: 'Object'},
            {dbName: 'NULL', dbType: 0, javaType: 'Object'},
            {dbName: 'OTHER', dbType: 1111, javaType: 'Object'},
            {dbName: 'JAVA_OBJECT', dbType: 2000, javaType: 'Object'},
            {dbName: 'DISTINCT', dbType: 2001, javaType: 'Object'},
            {dbName: 'STRUCT', dbType: 2002, javaType: 'Object'},
            {dbName: 'ARRAY', dbType: 2003, javaType: 'Object'},
            {dbName: 'BLOB', dbType: 2004, javaType: 'Object'},
            {dbName: 'CLOB', dbType: 2005, javaType: 'String'},
            {dbName: 'REF', dbType: 2006, javaType: 'Object'},
            {dbName: 'DATALINK', dbType: 70, javaType: 'Object'},
            {dbName: 'BOOLEAN', dbType: 16, javaType: 'Boolean', primitiveType: 'boolean'},
            {dbName: 'ROWID', dbType: -8, javaType: 'Object'},
            {dbName: 'NCHAR', dbType: -15, javaType: 'String'},
            {dbName: 'NVARCHAR', dbType: -9, javaType: 'String'},
            {dbName: 'LONGNVARCHAR', dbType: -16, javaType: 'String'},
            {dbName: 'NCLOB', dbType: 2011, javaType: 'String'},
            {dbName: 'SQLXML', dbType: 2009, javaType: 'Object'}
        ];

        /*eslint-disable */
        const JAVA_KEYWORDS = [
            'abstract',     'assert',        'boolean',      'break',           'byte',
            'case',         'catch',         'char',         'class',           'const',
            'continue',     'default',       'do',           'double',          'else',
            'enum',         'extends',       'false',        'final',           'finally',
            'float',        'for',           'goto',         'if',              'implements',
            'import',       'instanceof',    'int',          'interface',       'long',
            'native',       'new',           'null',         'package',         'private',
            'protected',    'public',        'return',       'short',           'static',
            'strictfp',     'super',         'switch',       'synchronized',    'this',
            'throw',        'throws',        'transient',    'true',            'try',
            'void',         'volatile',      'while'
        ];
        /*eslint-enable */

        const VALID_JAVA_IDENTIFIER = new RegExp('^[a-zA-Z_$][a-zA-Z\\d_$]*$');

        let popover = null;

        function isElementInViewport(el) {
            const rect = el.getBoundingClientRect();

            return (
                rect.top >= 0 &&
                rect.left >= 0 &&
                rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
                rect.right <= (window.innerWidth || document.documentElement.clientWidth)
            );
        }

        const _showPopoverMessage = (id, message, showTime) => {
            const body = $('body');

            let el = body.find('#' + id);

            if (!el || el.length === 0)
                el = body.find('[name="' + id + '"]');

            if (el && el.length > 0) {
                if (!isElementInViewport(el[0])) {
                    $location.hash(el[0].id);

                    $anchorScroll();
                }

                const newPopover = $popover(el, {content: message});

                popover = newPopover;

                $timeout(function() { newPopover.$promise.then(newPopover.show); }, 400);

                $timeout(function() { newPopover.hide(); }, showTime || 5000);
            }
        };

        function ensureActivePanel(ui, pnl, focusId) {
            if (ui) {
                const collapses = $('div.panel-collapse');

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
                        const newActivePanels = angular.copy(activePanels);

                        newActivePanels.push(idx);

                        ui.activePanels = newActivePanels;
                    }
                }

                if (isDefined(focusId))
                    $focus(focusId);
            }
        }

        function showPopoverMessage(ui, panelId, id, message, showTime) {
            if (popover)
                popover.hide();

            if (ui) {
                ensureActivePanel(ui, panelId);

                $timeout(() => _showPopoverMessage(id, message, showTime), ui.isPanelLoaded(panelId) ? 200 : 500);
            }
            else
                _showPopoverMessage(id, message, showTime);

            return false;
        }

        function isValidJavaIdentifier(msg, ident, elemId, panels, panelId) {
            if (isEmptyString(ident))
                return showPopoverMessage(panels, panelId, elemId, msg + ' is invalid!');

            if (_.includes(JAVA_KEYWORDS, ident))
                return showPopoverMessage(panels, panelId, elemId, msg + ' could not contains reserved java keyword: "' + ident + '"!');

            if (!VALID_JAVA_IDENTIFIER.test(ident))
                return showPopoverMessage(panels, panelId, elemId, msg + ' contains invalid identifier: "' + ident + '"!');

            return true;
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

        function getModel(obj, field) {
            let path = field.path;

            if (!isDefined(path) || !isDefined(obj))
                return obj;

            path = path.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
            path = path.replace(/^\./, '');           // strip a leading dot

            const segs = path.split('.');
            let root = obj;

            while (segs.length > 0) {
                const pathStep = segs.shift();

                if (typeof root[pathStep] === 'undefined')
                    root[pathStep] = {};

                root = root[pathStep];
            }

            return root;
        }

        function extractDataSource(cache) {
            if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
                const storeFactory = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];

                if (storeFactory.dialect || (storeFactory.connectVia === 'DataSource'))
                    return storeFactory;
            }

            return null;
        }

        const cacheStoreJdbcDialects = [
            {value: 'Generic', label: 'Generic JDBC'},
            {value: 'Oracle', label: 'Oracle'},
            {value: 'DB2', label: 'IBM DB2'},
            {value: 'SQLServer', label: 'Microsoft SQL Server'},
            {value: 'MySQL', label: 'MySQL'},
            {value: 'PostgreSQL', label: 'PostgreSQL'},
            {value: 'H2', label: 'H2 database'}
        ];

        function domainForStoreConfigured(domain) {
            const isEmpty = !isDefined(domain) || (isEmptyString(domain.databaseSchema) &&
                isEmptyString(domain.databaseTable) &&
                _.isEmpty(domain.keyFields) &&
                _.isEmpty(domain.valueFields));

            return !isEmpty;
        }

        const DS_CHECK_SUCCESS = { checked: true };

        function compareDataSources(firstCache, secondCache) {
            const firstDs = extractDataSource(firstCache);
            const secondDs = extractDataSource(secondCache);

            if (firstDs && secondDs) {
                const firstDB = firstDs.dialect;
                const secondDB = secondDs.dialect;

                if (firstDs.dataSourceBean === secondDs.dataSourceBean && firstDB !== secondDB)
                    return {checked: false, firstCache, firstDB, secondCache, secondDB};
            }

            return DS_CHECK_SUCCESS;
        }

        function toJavaName(prefix, name) {
            const javaName = name ? name.replace(/[^A-Za-z_0-9]+/g, '_') : 'dflt';

            return prefix + javaName.charAt(0).toLocaleUpperCase() + javaName.slice(1);
        }

        return {
            getModel,
            mkOptions(options) {
                return _.map(options, (option) => {
                    return {value: option, label: isDefined(option) ? option : 'Not set'};
                });
            },
            isDefined,
            hasProperty(obj, props) {
                for (const propName in props) {
                    if (props.hasOwnProperty(propName)) {
                        if (obj[propName])
                            return true;
                    }
                }

                return false;
            },
            isEmptyString,
            errorMessage,
            hideAlert() {
                if (msgModal)
                    msgModal.hide();
            },
            showError,
            showInfo(msg) {
                if (msgModal)
                    msgModal.hide();

                msgModal = $alert({
                    type: 'success',
                    title: msg,
                    duration: 2
                });

                msgModal.$scope.icon = 'fa-check-circle-o';
            },
            SUPPORTED_JDBC_TYPES,
            findJdbcType(jdbcType) {
                const res = _.find(ALL_JDBC_TYPES, function(item) {
                    return item.dbType === jdbcType;
                });

                return res ? res : {dbName: 'Unknown', javaType: 'Unknown'};
            },
            javaBuiltInClasses,
            javaBuiltInTypes,
            isJavaBuiltInClass,
            isValidJavaIdentifier,
            isValidJavaClass(msg, ident, allowBuiltInClass, elemId, packageOnly, panels, panelId) {
                if (isEmptyString(ident))
                    return showPopoverMessage(panels, panelId, elemId, msg + ' could not be empty!');

                const parts = ident.split('.');

                const len = parts.length;

                if (!allowBuiltInClass && isJavaBuiltInClass(ident))
                    return showPopoverMessage(panels, panelId, elemId, msg + ' should not be the Java build-in class!');

                if (len < 2 && !isJavaBuiltInClass(ident) && !packageOnly)
                    return showPopoverMessage(panels, panelId, elemId, msg + ' does not have package specified!');

                for (let i = 0; i < parts.length; i++) {
                    const part = parts[i];

                    if (!isValidJavaIdentifier(msg, part, elemId, panels, panelId))
                        return false;
                }

                return true;
            },
            domainForQueryConfigured(domain) {
                const isEmpty = !isDefined(domain) || (_.isEmpty(domain.fields) &&
                    _.isEmpty(domain.aliases) &&
                    _.isEmpty(domain.indexes));

                return !isEmpty;
            },
            domainForStoreConfigured,
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
            showPopoverMessage,
            hidePopover() {
                if (popover)
                    popover.hide();
            },
            confirmUnsavedChanges(dirty, selectFunc) {
                if (dirty) {
                    if ($window.confirm('You have unsaved changes.\n\nAre you sure you want to discard them?'))
                        selectFunc();
                }
                else
                    selectFunc();
            },
            saveBtnTipText(dirty, objectName) {
                if (dirty)
                    return 'Save ' + objectName;

                return 'Nothing to save';
            },
            download(type, name, data) {
                const file = document.createElement('a');

                file.setAttribute('href', 'data:' + type + ';charset=utf-8,' + data);
                file.setAttribute('download', name);
                file.setAttribute('target', '_self');

                file.style.display = 'none';

                document.body.appendChild(file);

                file.click();

                document.body.removeChild(file);
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
            getQueryVariable(name) {
                const attrs = window.location.search.substring(1).split('&');
                const attr = _.find(attrs, (a) => a === name || (a.indexOf('=') >= 0 && a.substr(0, a.indexOf('=')) === name));

                if (!isDefined(attr))
                    return null;

                if (attr === name)
                    return true;

                return attr.substr(attr.indexOf('=') + 1);
            },
            cacheStoreJdbcDialects,
            cacheStoreJdbcDialectsLabel(dialect) {
                const found = _.find(cacheStoreJdbcDialects, function(dialectVal) {
                    return dialectVal.value === dialect;
                });

                return found ? found.label : null;
            },
            checkCachesDataSources(caches, checkCacheExt) {
                let res = DS_CHECK_SUCCESS;

                _.find(caches, function(curCache, curIx) {
                    if (isDefined(checkCacheExt)) {
                        if (!isDefined(checkCacheExt._id) || checkCacheExt.id !== curCache._id) {
                            res = compareDataSources(checkCacheExt, curCache);

                            return !res.checked;
                        }

                        return false;
                    }

                    return _.find(caches, function(checkCache, checkIx) {
                        if (checkIx < curIx) {
                            res = compareDataSources(checkCache, curCache);

                            return !res.checked;
                        }

                        return false;
                    });
                });

                return res;
            },
            autoCacheStoreConfiguration(cache, domains) {
                const cacheStoreFactory = isDefined(cache.cacheStoreFactory) &&
                    isDefined(cache.cacheStoreFactory.kind);

                if (!cacheStoreFactory && _.findIndex(domains, domainForStoreConfigured) >= 0) {
                    const dflt = !cache.readThrough && !cache.writeThrough;

                    return {
                        cacheStoreFactory: {
                            kind: 'CacheJdbcPojoStoreFactory',
                            CacheJdbcPojoStoreFactory: {
                                dataSourceBean: toJavaName('ds', cache.name),
                                dialect: 'Generic'
                            }
                        },
                        readThrough: dflt || cache.readThrough,
                        writeThrough: dflt || cache.writeThrough
                    };
                }
            },
            autoClusterSwapSpiConfiguration(cluster, caches) {
                const swapConfigured = cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind;

                if (!swapConfigured && _.find(caches, function(cache) {
                    return cache.swapEnabled;
                }))
                    return {swapSpaceSpi: {kind: 'FileSwapSpaceSpi'}};

                return null;
            },
            randomString(len) {
                const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
                const possibleLen = possible.length;

                let res = '';

                for (let i = 0; i < len; i++)
                    res += possible.charAt(Math.floor(Math.random() * possibleLen));

                return res;
            }
        };
    }]);

// Confirm change location.
consoleModule.service('$unsavedChangesGuard', ['$rootScope', ($root) => {
    return {
        install($scope) {
            $scope.$on('$destroy', function() {
                window.onbeforeunload = null;
            });

            const unbind = $root.$on('$stateChangeStart', function(event) {
                if ($scope.ui && $scope.ui.inputForm && $scope.ui.inputForm.$dirty) {
                    if (!confirm('You have unsaved changes.\n\nAre you sure you want to discard them?')) // eslint-disable-line no-alert
                        event.preventDefault();
                    else
                        unbind();
                }
            });

            window.onbeforeunload = function() {
                return $scope.ui && $scope.ui.inputForm && $scope.ui.inputForm.$dirty
                    ? 'You have unsaved changes.\n\nAre you sure you want to discard them?'
                    : null;
            };
        }
    };
}]);

// Service for confirm or skip several steps.
consoleModule.service('$confirmBatch', ['$modal', '$rootScope', '$q', ($modal, $root, $q) => {
    const scope = $root.$new();

    scope.confirmModal = $modal({templateUrl: '/templates/batch-confirm.html', scope, placement: 'center', show: false});

    function _done(cancel) {
        scope.confirmModal.hide();

        if (cancel)
            scope.deferred.reject('cancelled');
        else
            scope.deferred.resolve();
    }

    function _nextElement(skip) {
        scope.items[scope.curIx++].skip = skip;

        if (scope.curIx < scope.items.length)
            scope.content = scope.contentGenerator(scope.items[scope.curIx]);
        else
            _done();
    }

    scope.cancel = function() {
        _done(true);
    };

    scope.skip = function(applyToAll) {
        if (applyToAll) {
            for (let i = scope.curIx; i < scope.items.length; i++)
                scope.items[i].skip = true;

            _done();
        }
        else
            _nextElement(true);
    };

    scope.overwrite = function(applyToAll) {
        if (applyToAll)
            _done();
        else
            _nextElement(false);
    };

    return {
        /**
         * Show confirm all dialog.
         *
             * @param confirmMessageFn Function to generate a confirm message.
         * @param itemsToConfirm Array of element to process by confirm.
         */
        confirm(confirmMessageFn, itemsToConfirm) {
            scope.deferred = $q.defer();

            scope.contentGenerator = confirmMessageFn;

            scope.items = itemsToConfirm;
            scope.curIx = 0;
            scope.content = (scope.items && scope.items.length > 0) ? scope.contentGenerator(scope.items[0]) : null;

            scope.confirmModal.$promise.then(scope.confirmModal.show);

            return scope.deferred.promise;
        }
    };
}]);

// 'Clone' popup service.
consoleModule.service('$clone', ['$modal', '$rootScope', '$q', ($modal, $root, $q) => {
    const scope = $root.$new();

    let _names = [];
    let deferred;
    let _validator;

    function _nextAvailableName(name) {
        let num = 1;
        let tmpName = name;

        while (_.includes(_names, tmpName)) {
            tmpName = name + '_' + num.toString();

            num++;
        }

        return tmpName;
    }

    const cloneModal = $modal({templateUrl: '/templates/clone.html', scope, placement: 'center', show: false});

    scope.ok = function(newName) {
        if (!_validator || _validator(newName)) {
            deferred.resolve(_nextAvailableName(newName));

            cloneModal.hide();
        }
    };

    cloneModal.confirm = function(oldName, names, validator) {
        _names = names;

        scope.newName = _nextAvailableName(oldName);

        _validator = validator;

        deferred = $q.defer();

        cloneModal.$promise.then(cloneModal.show);

        return deferred.promise;
    };

    return cloneModal;
}]);

// Tables support service.
consoleModule.service('$table', ['$common', '$focus', function($common, $focus) {
    function _model(item, field) {
        return $common.getModel(item, field);
    }

    const table = {name: 'none', editIndex: -1};

    function _tableReset() {
        delete table.field;
        table.name = 'none';
        table.editIndex = -1;

        $common.hidePopover();
    }

    function _tableSaveAndReset() {
        const field = table.field;

        const save = $common.isDefined(field) && $common.isDefined(field.save);

        if (!save || !$common.isDefined(field) || field.save(field, table.editIndex, true)) {
            _tableReset();

            return true;
        }

        return false;
    }

    function _tableState(field, editIndex, specName) {
        table.field = field;
        table.name = specName || field.model;
        table.editIndex = editIndex;
    }

    function _tableUI(field) {
        const ui = field.ui;

        return ui ? ui : field.type;
    }

    function _tableFocus(focusId, index) {
        $focus((index < 0 ? 'new' : 'cur') + focusId + (index >= 0 ? index : ''));
    }

    function _tablePairValue(filed, index) {
        return index < 0 ? {key: filed.newKey, value: filed.newValue} : {key: filed.curKey, value: filed.curValue};
    }

    function _tableStartEdit(item, tbl, index, save) {
        _tableState(tbl, index);

        const val = _.get(_model(item, tbl), tbl.model)[index];

        const ui = _tableUI(tbl);

        tbl.save = save;

        if (ui === 'table-pair') {
            tbl.curKey = val[tbl.keyName];
            tbl.curValue = val[tbl.valueName];

            _tableFocus('Key' + tbl.focusId, index);
        }
        else if (ui === 'table-db-fields') {
            tbl.curDatabaseFieldName = val.databaseFieldName;
            tbl.curDatabaseFieldType = val.databaseFieldType;
            tbl.curJavaFieldName = val.javaFieldName;
            tbl.curJavaFieldType = val.javaFieldType;

            _tableFocus('DatabaseFieldName' + tbl.focusId, index);
        }
        else if (ui === 'table-indexes') {
            tbl.curIndexName = val.name;
            tbl.curIndexType = val.indexType;
            tbl.curIndexFields = val.fields;

            _tableFocus(tbl.focusId, index);
        }
    }

    function _tableNewItem(tbl) {
        _tableState(tbl, -1);

        const ui = _tableUI(tbl);

        if (ui === 'table-pair') {
            tbl.newKey = null;
            tbl.newValue = null;

            _tableFocus('Key' + tbl.focusId, -1);
        }
        else if (ui === 'table-db-fields') {
            tbl.newDatabaseFieldName = null;
            tbl.newDatabaseFieldType = null;
            tbl.newJavaFieldName = null;
            tbl.newJavaFieldType = null;

            _tableFocus('DatabaseFieldName' + tbl.focusId, -1);
        }
        else if (ui === 'table-indexes') {
            tbl.newIndexName = null;
            tbl.newIndexType = 'SORTED';
            tbl.newIndexFields = null;

            _tableFocus(tbl.focusId, -1);
        }
    }

    return {
        tableState: _tableState,
        tableReset: _tableReset,
        tableSaveAndReset: _tableSaveAndReset,
        tableNewItem: _tableNewItem,
        tableNewItemActive(tbl) {
            return table.name === tbl.model && table.editIndex < 0;
        },
        tableEditing(tbl, index) {
            return table.name === tbl.model && table.editIndex === index;
        },
        tableEditedRowIndex() {
            return table.editIndex;
        },
        tableField() {
            return table.field;
        },
        tableStartEdit: _tableStartEdit,
        tableRemove(item, field, index) {
            _tableReset();

            _model(item, field)[field.model].splice(index, 1);
        },
        tablePairValue: _tablePairValue,
        tablePairSave(pairValid, item, field, index, stopEdit) {
            const valid = pairValid(item, field, index);

            if (valid) {
                const pairValue = _tablePairValue(field, index);

                let pairModel = {};

                const container = _.get(item, field.model);

                if (index < 0) {
                    pairModel[field.keyName] = pairValue.key;
                    pairModel[field.valueName] = pairValue.value;

                    if (container)
                        container.push(pairModel);
                    else
                        _.set(item, field.model, [pairModel]);

                    if (!stopEdit)
                        _tableNewItem(field);
                }
                else {
                    pairModel = container[index];

                    pairModel[field.keyName] = pairValue.key;
                    pairModel[field.valueName] = pairValue.value;

                    if (!stopEdit) {
                        if (index < container.length - 1)
                            _tableStartEdit(item, field, index + 1);
                        else
                            _tableNewItem(field);
                    }
                }
            }

            return valid;
        },
        tablePairSaveVisible(field, index) {
            const pairValue = _tablePairValue(field, index);

            return !$common.isEmptyString(pairValue.key) && !$common.isEmptyString(pairValue.value);
        },
        tableFocusInvalidField(index, id) {
            _tableFocus(id, index);

            return false;
        },
        tableFieldId(index, id) {
            return (index < 0 ? 'new' : 'cur') + id + (index >= 0 ? index : '');
        }
    };
}]);

consoleModule.service('ngCopy', ['$window', '$common', function($window, $common) {
    const body = angular.element($window.document.body);

    const textArea = angular.element('<textarea/>');

    textArea.css({
        position: 'fixed',
        opacity: '0'
    });

    return function(toCopy) {
        textArea.val(toCopy);

        body.append(textArea);

        textArea[0].select();

        try {
            if (document.execCommand('copy'))
                $common.showInfo('Value copied to clipboard');
            else
                window.prompt('Copy to clipboard: Ctrl+C, Enter', toCopy);  // eslint-disable-line no-alert
        } catch (err) {
            window.prompt('Copy to clipboard: Ctrl+C, Enter', toCopy);  // eslint-disable-line no-alert
        }

        textArea.remove();
    };
}]).directive('ngClickCopy', ['ngCopy', function(ngCopy) {
    return {
        restrict: 'A',
        link(scope, element, attrs) {
            element.bind('click', function() {
                ngCopy(attrs.ngClickCopy);
            });

            if (!document.queryCommandSupported('copy'))
                element.hide();
        }
    };
}]);

// Filter domain models with key fields configuration.
consoleModule.filter('domainsValidation', ['$common', function($common) {
    return function(domains, valid, invalid) {
        if (valid && invalid)
            return domains;

        const out = [];

        _.forEach(domains, function(domain) {
            const _valid = !$common.domainForStoreConfigured(domain) || $common.isJavaBuiltInClass(domain.keyType) || !_.isEmpty(domain.keyFields);

            if (valid && _valid || invalid && !_valid)
                out.push(domain);
        });

        return out;
    };
}]);

// Directive to enable validation to match specified value.
consoleModule.directive('match', ['$parse', ($parse) => {
    return {
        require: 'ngModel',
        link(scope, elem, attrs, ctrl) {
            scope.$watch(() => $parse(attrs.match)(scope) === ctrl.$modelValue, (currentValue) => ctrl.$setValidity('mismatch', currentValue));
        }
    };
}]);

// Directive to bind ENTER key press with some user action.
consoleModule.directive('onEnter', ['$timeout', ($timeout) => {
    return function(scope, elem, attrs) {
        elem.on('keydown keypress', function(event) {
            if (event.which === 13) {
                scope.$apply(function() {
                    $timeout(() => scope.$eval(attrs.onEnter));
                });

                event.preventDefault();
            }
        });

        // Removes bound events in the element itself when the scope is destroyed.
        scope.$on('$destroy', function() {
            elem.off('keydown keypress');
        });
    };
}]);

// Directive to bind ESC key press with some user action.
consoleModule.directive('onEscape', () => {
    return function(scope, elem, attrs) {
        elem.on('keydown keypress', function(event) {
            if (event.which === 27) {
                scope.$apply(function() {
                    scope.$eval(attrs.onEscape);
                });

                event.preventDefault();
            }
        });

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', function() {
            elem.off('keydown keypress');
        });
    };
});

// Directive to retain selection. To fix angular-strap typeahead bug with setting cursor to the end of text.
consoleModule.directive('retainSelection', ['$timeout', ($timeout) => {
    let promise;

    return function(scope, elem) {
        elem.on('keydown', function(evt) {
            const key = evt.which;
            const ctrlDown = evt.ctrlKey || evt.metaKey;
            const input = this;
            let start = input.selectionStart;

            if (promise)
                $timeout.cancel(promise);

            promise = $timeout(function() {
                let setCursor = false;

                // Handle Backspace[8].
                if (key === 8 && start > 0) {
                    start -= 1;

                    setCursor = true;
                }
                // Handle Del[46].
                else if (key === 46)
                    setCursor = true;
                // Handle: Caps Lock[20], Tab[9], Shift[16], Ctrl[17], Alt[18], Esc[27], Enter[13], Arrows[37..40], Home[36], End[35], Ins[45], PgUp[33], PgDown[34], F1..F12[111..124], Num Lock[], Scroll Lock[145].
                else if (!(key === 8 || key === 9 || key === 13 || (key > 15 && key < 20) || key === 27 ||
                    (key > 32 && key < 41) || key === 45 || (key > 111 && key < 124) || key === 144 || key === 145)) {
                    // Handle: Ctrl + [A[65], C[67], V[86]].
                    if (!(ctrlDown && (key === 65 || key === 67 || key === 86))) {
                        start += 1;

                        setCursor = true;
                    }
                }

                if (setCursor)
                    input.setSelectionRange(start, start);

                promise = null;
            });
        });

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', function() {
            elem.off('keydown');
        });
    };
}]);

// Factory function to focus element.
consoleModule.factory('$focus', ['$timeout', ($timeout) => {
    return function(id) {
        // Timeout makes sure that is invoked after any other event has been triggered.
        // E.g. click events that need to run before the focus or inputs elements that are
        // in a disabled state but are enabled when those events are triggered.
        $timeout(function() {
            const elem = $('#' + id);

            if (elem.length > 0)
                elem[0].focus();
        }, 100);
    };
}]);

// Directive to auto-focus element.
consoleModule.directive('autoFocus', ['$timeout', ($timeout) => {
    return {
        restrict: 'AC',
        link(scope, element) {
            $timeout(() => element[0].focus());
        }
    };
}]);

// Directive to focus next element on ENTER key.
consoleModule.directive('enterFocusNext', ['$focus', ($focus) => {
    return function(scope, elem, attrs) {
        elem.on('keydown keypress', function(event) {
            if (event.which === 13) {
                event.preventDefault();

                $focus(attrs.enterFocusNext);
            }
        });
    };
}]);

// Directive to mark elements to focus.
consoleModule.directive('onClickFocus', ['$focus', ($focus) => {
    return function(scope, elem, attr) {
        elem.on('click', function() {
            $focus(attr.onClickFocus);
        });

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', function() {
            elem.off('click');
        });
    };
}]);

consoleModule.controller('resetPassword', [
    '$scope', '$modal', '$http', '$common', '$focus', 'Auth', '$state',
    ($scope, $modal, $http, $common, $focus, Auth, $state) => {
        if ($state.params.token) {
            $http.post('/api/v1/password/validate/token', {token: $state.params.token})
                .success((res) => {
                    $scope.email = res.email;
                    $scope.token = res.token;
                    $scope.error = res.error;

                    if ($scope.token && !$scope.error)
                        $focus('user_password');
                });
        }

        // Try to reset user password for provided token.
        $scope.resetPassword = function(reset_info) {
            $http.post('/api/v1/password/reset', reset_info)
                .success(function() {
                    $common.showInfo('Password successfully changed');

                    $state.go('base.configuration.clusters');
                })
                .error(function(data, state) {
                    $common.showError(data);

                    if (state === 503)
                        $state.go('base.configuration.clusters');
                });
        };
    }
]);

// Sign in controller.
// TODO IGNITE-1936 Refactor this controller.
consoleModule.controller('auth', ['$scope', '$focus', 'Auth', 'IgniteCountries', ($scope, $focus, Auth, Countries) => {
    $scope.auth = Auth.auth;
    $scope.forgotPassword = Auth.forgotPassword;
    $scope.action = 'signin';
    $scope.countries = Countries.getAll();

    $focus('user_email');
}]);

// Navigation bar controller.
consoleModule.controller('notebooks', ['$rootScope', '$scope', '$modal', '$state', '$http', '$common',
    ($root, $scope, $modal, $state, $http, $common) => {
        $root.notebooks = [];

        // Pre-fetch modal dialogs.
        const _notebookNewModal = $modal({scope: $scope, templateUrl: '/sql/notebook-new.html', show: false});

        $root.rebuildDropdown = function() {
            $scope.notebookDropdown = [
                {text: 'Create new notebook', click: 'inputNotebookName()'},
                {divider: true}
            ];

            _.forEach($root.notebooks, function(notebook) {
                $scope.notebookDropdown.push({
                    text: notebook.name,
                    sref: 'base.sql.notebook({noteId:"' + notebook._id + '"})'
                });
            });
        };

        $root.reloadNotebooks = function() {
            // When landing on the page, get clusters and show them.
            $http.post('/api/v1/notebooks/list')
                .success(function(data) {
                    $root.notebooks = data;

                    $root.rebuildDropdown();
                })
                .error(function(errMsg) {
                    $common.showError(errMsg);
                });
        };

        $root.inputNotebookName = function() {
            _notebookNewModal.$promise.then(_notebookNewModal.show);
        };

        $root.createNewNotebook = function(name) {
            $http.post('/api/v1/notebooks/new', {name})
                .success(function(noteId) {
                    _notebookNewModal.hide();

                    $root.reloadNotebooks();

                    $state.go('base.sql.notebook', {noteId});
                })
                .error(function(message) {
                    $common.showError(message);
                });
        };

        $root.reloadNotebooks();
    }
]);

export default consoleModule;
