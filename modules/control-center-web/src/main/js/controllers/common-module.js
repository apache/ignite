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

var consoleModule = angular.module('ignite-web-console',
    [
        'ngAnimate', 'ngSanitize', 'mgcrea.ngStrap', 'smart-table', 'ui.ace', 'treeControl', 'darthwade.dwLoading', 'ui.grid', 'ui.grid.autoResize', 'ui.grid.exporter', 'nvd3', 'dndLists'
        /* ignite:modules */
        , 'ignite-console'
        , 'ignite-console.navbar'
        , 'ignite-console.configuration.sidebar'
        /* endignite */
        /* ignite:plugins */
        /* endignite */
    ])
    .run(function ($rootScope, $http, $state, $common, Auth, User) {
        if (Auth.authorized)
            User.read().then(function (user) {
                $rootScope.$broadcast('user', user);
            });

        $rootScope.revertIdentity = function () {
            $http
                .get('/api/v1/admin/revert/identity')
                .then(User.read)
                .then(function (user) {
                    $rootScope.$broadcast('user', user);

                    $state.go('base.admin');
                })
                .catch(function (errMsg) {
                    $common.showError($common.errorMessage(errMsg));
                });
        };
    });

// Modal popup configuration.
consoleModule.config(function ($modalProvider) {
    angular.extend($modalProvider.defaults, {
        html: true
    });
});

// Comboboxes configuration.
consoleModule.config(function ($popoverProvider) {
    angular.extend($popoverProvider.defaults, {
        trigger: 'manual',
        placement: 'right',
        container: 'body',
        templateUrl: '/templates/validation-error.html'
    });
});

// Tooltips configuration.
consoleModule.config(function ($tooltipProvider) {
    angular.extend($tooltipProvider.defaults, {
        container: 'body',
        delay: 150,
        placement: 'right',
        html: 'true',
        trigger: 'click hover'
    });
});

// Comboboxes configuration.
consoleModule.config(function ($selectProvider) {
    angular.extend($selectProvider.defaults, {
        container: 'body',
        maxLength: '5',
        allText: 'Select All',
        noneText: 'Clear All',
        templateUrl: '/templates/select.html',
        iconCheckmark: 'fa fa-check',
        caretHtml: ''
    });
});

// Alerts configuration.
consoleModule.config(function ($alertProvider) {
    angular.extend($alertProvider.defaults, {
        container: 'body',
        placement: 'top-right',
        duration: '5',
        type: 'danger'
    });
});

// Modals configuration.
consoleModule.config(function($modalProvider) {
    angular.extend($modalProvider.defaults, {
        animation: 'am-fade-and-scale'
    });
});

// Common functions to be used in controllers.
consoleModule.service('$common', [
    '$alert', '$popover', '$timeout', '$focus', '$window', function ($alert, $popover, $timeout, $focus, $window) {
        function isDefined(v) {
            return !(v === undefined || v === null);
        }

        function isEmptyArray(arr) {
            if (isDefined(arr))
                return arr.length === 0;

            return true;
        }

        function isEmptyString(s) {
            if (isDefined(s))
                return s.trim().length === 0;

            return true;
        }

        var msgModal;

        function errorMessage(errMsg) {
            if (errMsg) {
                if (errMsg.hasOwnProperty('message'))
                    return errMsg.message;

                return errMsg;
            }

            return 'Internal server error.';
        }

        function showError(msg, placement, container, persistent) {
            if (msgModal)
                msgModal.hide();

            msgModal = $alert({
                title: errorMessage(msg),
                placement: placement ? placement : 'top-right',
                container: container ? container : 'body'
            });

            if (persistent)
                msgModal.$options.duration = false;

            return false;
        }

        var javaBuildInClasses = [
            'BigDecimal', 'Boolean', 'Byte', 'Date', 'Double', 'Float', 'Integer', 'Long', 'Short', 'String', 'Time', 'Timestamp', 'UUID'
        ];

        var javaBuildInFullNameClasses = [
            'java.math.BigDecimal', 'java.lang.Boolean', 'java.lang.Byte', 'java.sql.Date', 'java.lang.Double',
            'java.lang.Float', 'java.lang.Integer', 'java.lang.Long', 'java.lang.Short', 'java.lang.String',
            'java.sql.Time', 'java.sql.Timestamp', 'java.util.UUID'
        ];

        function isJavaBuildInClass(cls) {
            if (isEmptyString(cls))
                return false;

            return _.contains(javaBuildInClasses, cls) || _.contains(javaBuildInFullNameClasses, cls);
        }

        var SUPPORTED_JDBC_TYPES = [
            'BIGINT',
            'BIT',
            'BOOLEAN',
            'CHAR',
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

        var ALL_JDBC_TYPES = [
            {dbName: 'BIT', dbType: -7, javaType: 'Boolean'},
            {dbName: 'TINYINT', dbType: -6, javaType: 'Byte'},
            {dbName: 'SMALLINT', dbType:  5, javaType: 'Short'},
            {dbName: 'INTEGER', dbType: 4, javaType: 'Integer'},
            {dbName: 'BIGINT', dbType: -5, javaType: 'Long'},
            {dbName: 'FLOAT', dbType: 6, javaType: 'Float'},
            {dbName: 'REAL', dbType: 7, javaType: 'Double'},
            {dbName: 'DOUBLE', dbType: 8, javaType: 'Double'},
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
            {dbName: 'BOOLEAN', dbType: 16, javaType: 'Boolean'},
            {dbName: 'ROWID', dbType: -8, javaType: 'Object'},
            {dbName: 'NCHAR', dbType: -15, javaType: 'String'},
            {dbName: 'NVARCHAR', dbType: -9, javaType: 'String'},
            {dbName: 'LONGNVARCHAR', dbType: -16, javaType: 'String'},
            {dbName: 'NCLOB', dbType: 2011, javaType: 'String'},
            {dbName: 'SQLXML', dbType: 2009, javaType: 'Object'}
        ];

        var JAVA_KEYWORDS = [
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

        var VALID_JAVA_IDENTIFIER = new RegExp('^[a-zA-Z_$][a-zA-Z\\d_$]*$');

        function isValidJavaIdentifier(msg, ident, elemId, panels, panelId) {
            if (isEmptyString(ident))
                return showPopoverMessage(panels, panelId, elemId, msg + ' is invalid!');

            if (_.contains(JAVA_KEYWORDS, ident))
                return showPopoverMessage(panels, panelId, elemId, msg + ' could not contains reserved java keyword: "' + ident + '"!');

            if (!VALID_JAVA_IDENTIFIER.test(ident))
                return showPopoverMessage(panels, panelId, elemId, msg + ' contains invalid identifier: "' + ident + '"!');

            return true;
        }

        var context = null;

        /**
         * Calculate width of specified text in body's font.
         *
         * @param text Text to calculate width.
         * @returns {Number} Width of text in pixels.
         */
        function measureText(text) {
            if (!context) {
                var canvas = document.createElement('canvas');

                context = canvas.getContext('2d');

                var style = window.getComputedStyle(document.getElementsByTagName('body')[0]);

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
            for (var nameIx = 0; nameIx < names.length; nameIx ++) {
                var s = names[nameIx];

                if (s.length > nameLength) {
                    var totalLength = s.length;

                    var packages = s.split('.');

                    var packageCnt = packages.length - 1;

                    for (var i = 0; i < packageCnt && totalLength > nameLength; i++) {
                        if (packages[i].length > 0) {
                            totalLength -= packages[i].length - 1;

                            packages[i] = packages[i][0];
                        }
                    }

                    if (totalLength > nameLength) {
                        var className = packages[packageCnt];

                        var classNameLen = className.length;

                        var remains = Math.min(nameLength - totalLength + classNameLen, classNameLen);

                        if (remains < 3)
                            remains = Math.min(3, classNameLen);

                        packages[packageCnt] = className.substring(0, remains) + '...';
                    }

                    var result = packages[0];

                    for (i = 1; i < packages.length; i++)
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

            var fitted = [];

            var widthByName = [];

            var len = names.length;

            var divideTo = len;

            for (var nameIx = 0; nameIx < len; nameIx ++) {
                fitted[nameIx] = false;

                widthByName[nameIx] = nameWidth;
            }

            // Try to distribute space from short class names to long class names.
            do {
                var remains = 0;

                for (nameIx = 0; nameIx < len; nameIx++) {
                    if (!fitted[nameIx]) {
                        var curNameWidth = measureText(names[nameIx]);

                        if (widthByName[nameIx] > curNameWidth) {
                            fitted[nameIx] = true;

                            remains += widthByName[nameIx] - curNameWidth;

                            divideTo -= 1;

                            widthByName[nameIx] = curNameWidth;
                        }
                    }
                }

                var remainsByName = remains / divideTo;

                for (nameIx = 0; nameIx < len; nameIx++) {
                    if (!fitted[nameIx]) {
                        widthByName[nameIx] += remainsByName;
                    }
                }
            }
            while(remains > 0);

            // Compact class names to available for each space.
            for (nameIx = 0; nameIx < len; nameIx ++) {
                var s = names[nameIx];

                if (s.length > (nameLength / 2 | 0)) {
                    var totalWidth = measureText(s);

                    if (totalWidth > widthByName[nameIx]) {
                        var packages = s.split('.');

                        var packageCnt = packages.length - 1;

                        for (var i = 0; i < packageCnt && totalWidth > widthByName[nameIx]; i++) {
                            if (packages[i].length > 1) {
                                totalWidth -= measureText(packages[i].substring(1, packages[i].length));

                                packages[i] = packages[i][0];
                            }
                        }

                        var shortPackage = '';

                        for (i = 0; i < packageCnt; i++)
                            shortPackage += packages[i] + '.';

                        var className = packages[packageCnt];

                        var classLen = className.length;

                        var minLen = Math.min(classLen, 3);

                        totalWidth = measureText(shortPackage + className);

                        // Compact class name if shorten package path is very long.
                        if (totalWidth > widthByName[nameIx]) {
                            var maxLen = classLen;

                            var middleLen = (minLen + (maxLen - minLen) / 2 ) | 0;

                            var minLenPx = measureText(shortPackage + className.substr(0, minLen) + '...');
                            var maxLenPx = totalWidth;

                            while (middleLen !== minLen && middleLen !== maxLen) {
                                var middleLenPx = measureText(shortPackage + className.substr(0, middleLen) + '...');

                                if (middleLenPx > widthByName[nameIx]) {
                                    maxLen = middleLen;
                                    maxLenPx = middleLenPx;
                                }
                                else {
                                    minLen = middleLen;
                                    minLenPx = middleLenPx;
                                }

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

            var totalWidth = measureText(label);

            if (totalWidth > nameWidth) {
                var maxLen = label.length;

                var minLen = Math.min(maxLen, 3);

                var middleLen = (minLen + (maxLen - minLen) / 2 ) | 0;

                var minLenPx = measureText(label.substr(0, minLen) + '...');
                var maxLenPx = totalWidth;

                while (middleLen !== minLen && middleLen !== maxLen) {
                    var middleLenPx = measureText(label.substr(0, middleLen) + '...');

                    if (middleLenPx > nameWidth) {
                        maxLen = middleLen;
                        maxLenPx = middleLenPx;
                    }
                    else {
                        minLen = middleLen;
                        minLenPx = middleLenPx;
                    }

                    middleLen = (minLen + (maxLen - minLen) / 2 ) | 0;
                }

                return label.substring(0, middleLen) + '...';
            }

            return label;
        }

        /**
         * Calculate available width for text in link to edit element.
         *
         * @param index Showed index of element for calcuraion maximal width in pixel.
         * @param id Id of contains link table.
         * @returns {*[]} First element is length of class for single value, second element is length for pair vlaue.
         */
        function availableWidth(index, id) {
            var idElem = $('#' + id);

            var width = 0;

            switch (idElem.prop("tagName")) {
                // Detection of available width in presentation table row.
                case 'TABLE':
                    var cont = $(idElem.find('tr')[index - 1]).find('td')[0];

                    width = cont.clientWidth;

                    if (width > 0) {
                        var children = $(cont).children(':not("a")');

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
            }

            return width | 0;
        }

        var popover = null;

        function ensureActivePanel(panels, id, focusId) {
            if (panels) {
                var idx = _.findIndex($('div.panel-collapse'), function(pnl) {
                    return pnl.id === id;
                });

                if (idx >= 0) {
                    var activePanels = panels.activePanels;

                    if (!activePanels || activePanels.length < 1)
                        panels.activePanels = [idx];
                    else if (!_.contains(activePanels, idx)) {
                        var newActivePanels = angular.copy(activePanels);

                        newActivePanels.push(idx);

                        panels.activePanels = newActivePanels;
                    }
                }

                if (isDefined(focusId))
                    $focus(focusId);
            }
        }

        function showPopoverMessage(panels, panelId, id, message) {
            popoverShown = false;

            ensureActivePanel(panels, panelId, id);

            var el = $('body').find('#' + id);

            if (popover)
                popover.hide();

            var newPopover = $popover(el, {content: message});

            popover = newPopover;

            $timeout(function () { newPopover.$promise.then(newPopover.show); }, 400);

            $timeout(function () { newPopover.hide(); }, 5000);

            return false;
        }

        function getModel(obj, field) {
            var path = field.path;

            if (!isDefined(path) || !isDefined(obj))
                return obj;

            path = path.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
            path = path.replace(/^\./, '');           // strip a leading dot

            var segs = path.split('.');
            var root = obj;

            while (segs.length > 0) {
                var pathStep = segs.shift();

                if (typeof root[pathStep] === 'undefined')
                    root[pathStep] = {};

                root = root[pathStep];
            }

            return root;
        }

        function checkGroupDirty(group, curItem, srcItem) {
            function _compareField(field) {
                var curModel = getModel(curItem, field);
                var srcModel = getModel(srcItem, field);

                if (field.model) {
                    if (field.model === 'kind' && isDefined(curModel.kind)) {
                        if (curModel.kind !== srcModel.kind)
                            return true;

                        if (_compareFields(field.details[curModel.kind].fields))
                            return true;
                    }

                    var curValue = curModel[field.model];
                    var srcValue = srcModel[field.model];

                    if ((_.isArray(curValue) || _.isString(curValue)) && (curValue.length === 0) && (srcValue === undefined))
                        curValue = undefined;

                    if (_.isBoolean(curValue) && !curValue && srcValue === undefined)
                        curValue = undefined;

                    var isCur = isDefined(curValue);
                    var isSrc = isDefined(srcValue);

                    return !!((isCur && !isSrc) || (!isCur && isSrc) || (isCur && isSrc && !angular.equals(curValue, srcValue)));
                }
                else if (field.type === 'panel-details' &&  _compareFields(field.details))
                    return true;

                return false;
            }

            function _compareFields(fields) {
                return _.findIndex(fields, _compareField) >= 0;
            }

            group.dirty = _compareFields(group.fields);

            return group.dirty;
        }

        return {
            getModel: getModel,
            joinTip: function (arr) {
                if (!arr)
                    return arr;

                var lines = _.map(arr, function (line) {
                    var rtrimmed = line.replace(/\s+$/g, '');

                    if (rtrimmed.indexOf('>', this.length - 1) === -1)
                        rtrimmed = rtrimmed + '<br/>';

                    return rtrimmed;
                });

                return lines.join('');
            },
            mkOptions: function (options) {
                return _.map(options, function (option) {
                    return {value: option, label: isDefined(option) ? option : 'Not set'};
                });
            },
            isDefined: isDefined,
            hasProperty: function (obj, props) {
                for (var propName in props) {
                    if (props.hasOwnProperty(propName)) {
                        if (obj[propName])
                            return true;
                    }
                }

                return false;
            },
            isEmptyArray: isEmptyArray,
            isEmptyString: isEmptyString,
            errorMessage: errorMessage,
            hideAlert: function () {
                if (msgModal)
                    msgModal.hide();
            },
            showError: showError,
            showInfo: function (msg) {
                if (msgModal)
                    msgModal.hide();

                msgModal = $alert({
                    type: 'success',
                    title: msg,
                    duration: 2
                });
            },
            SUPPORTED_JDBC_TYPES: SUPPORTED_JDBC_TYPES,
            findJdbcType: function (jdbcType) {
                var res =  _.find(ALL_JDBC_TYPES, function (item) {
                    return item.dbType === jdbcType;
                });

                return res ? res : {dbName: 'Unknown', javaType: 'Unknown'};
            },
            javaBuildInClasses: javaBuildInClasses,
            isJavaBuildInClass: isJavaBuildInClass,
            isValidJavaIdentifier: isValidJavaIdentifier,
            isValidJavaClass: function (msg, ident, allowBuildInClass, elemId, packageOnly, panels, panelId) {
                if (isEmptyString(ident))
                    return showPopoverMessage(panels, panelId, elemId, msg + ' could not be empty!');

                var parts = ident.split('.');

                var len = parts.length;

                if (!allowBuildInClass && isJavaBuildInClass(ident))
                    return showPopoverMessage(panels, panelId, elemId, msg + ' should not be the Java build-in class!');

                if (len < 2 && !isJavaBuildInClass(ident) && !packageOnly)
                    return showPopoverMessage(panels, panelId, elemId, msg + ' does not have package specified!');

                for (var i = 0; i < parts.length; i++) {
                    var part = parts[i];

                    if (!isValidJavaIdentifier(msg, part, elemId, panels, panelId))
                        return false;
                }

                return true;
            },
            metadataForQueryConfigured: function (meta) {
                var isEmpty = !isDefined(meta) || (isEmptyArray(meta.fields) &&
                    isEmptyArray(meta.aliases) &&
                    isEmptyArray(meta.indexes));

                return !isEmpty;
            },
            metadataForStoreConfigured: function (meta) {
                var isEmpty = !isDefined(meta) || (isEmptyString(meta.databaseSchema) &&
                    isEmptyString(meta.databaseTable) &&
                    isEmptyArray(meta.keyFields) &&
                    isEmptyArray(meta.valueFields));

                return !isEmpty;
            },
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
            compactJavaName: function (id, index, maxLength, names, divider) {
                divider = ' ' + divider + ' ';

                var prefix = index + ') ';

                var nameCnt = names.length;

                var nameLength = ((maxLength - 3 * (nameCnt - 1)) / nameCnt) | 0;

                try {
                    var nameWidth = (availableWidth(index, id) - measureText(prefix) - (nameCnt - 1) * measureText(divider)) /
                        nameCnt | 0;

                    // HTML5 calculation of showed message width.
                    names = compactByMaxPixels(names, nameLength, nameWidth);
                }
                catch (err) {
                    names = compactByMaxCharts(names, nameLength);
                }

                var result = prefix + names[0];

                for (var nameIx = 1; nameIx < names.length; nameIx ++)
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
            compactTableLabel: function (id, index, maxLength, label) {
                label = index + ') ' + label;

                try {
                    var nameWidth = availableWidth(index, id) | 0;

                    // HTML5 calculation of showed message width.
                    label = compactLabelByPixels(label, nameWidth);
                }
                catch (err) {
                    var nameLength = maxLength - 3 | 0;

                    label = label.length > maxLength ? label.substr(0, nameLength) + '...' : label;
                }

                return label;
            },
            widthIsSufficient: function(id, index, text) {
                try {
                    var available = availableWidth(index, id);

                    var required = measureText(text);

                    return !available || available >= Math.floor(required);
                }
                catch (err) {
                    return true;
                }
            },
            ensureActivePanel: function (panels, id, focusId) {
                ensureActivePanel(panels, id, focusId);
            },
            panelExpanded: function (panels, id) {
                if (panels && panels.activePanels && panels.activePanels.length > 0) {
                    var idx = _.findIndex($('div.panel-collapse'), function(pnl) {
                        return pnl.id === id;
                    });

                    return idx >= 0 && _.includes(panels.activePanels, idx);
                }

                return false;
            },
            showPopoverMessage: function (panels, panelId, id, message) {
                return showPopoverMessage(panels, panelId, id, message);
            },
            hidePopover: function () {
                if (popover)
                    popover.hide();
            },
            confirmUnsavedChanges: function(dirty, selectFunc) {
                if (dirty) {
                    if ($window.confirm('You have unsaved changes.\n\nAre you sure you want to discard them?'))
                        selectFunc();
                }
                else
                    selectFunc();

            },
            saveBtnTipText: function (dirty, objectName) {
                if (dirty)
                    return 'Save ' + objectName;

                return 'Nothing to save';
            },
            download: function (type, name, data) {
                var file = document.createElement('a');

                file.setAttribute('href', 'data:' + type +';charset=utf-8,' + data);
                file.setAttribute('download', name);
                file.setAttribute('target', '_self');

                file.style.display = 'none';

                document.body.appendChild(file);

                file.click();

                document.body.removeChild(file);
            },
            resetItem: function (backupItem, selectedItem, groups, group) {
                function restoreFields(fields) {
                    // Reset fields by one.
                    for (var fldIx = 0; fldIx < fields.length; fldIx ++) {
                        var field = fields[fldIx];

                        if (field.model) {
                            var destMdl = getModel(backupItem, field);

                            if (isDefined(destMdl)) {
                                if (isDefined(selectedItem)) {
                                    var srcMdl = getModel(selectedItem, field);

                                    if (isDefined(srcMdl)) {
                                        // For array create copy.
                                        if ($.isArray(srcMdl[field.model]))
                                            destMdl[field.model] = srcMdl[field.model].slice();
                                        else
                                            destMdl[field.model] = srcMdl[field.model];
                                    }
                                    else
                                        destMdl[field.model] = undefined;
                                }
                                else
                                    destMdl[field.model] = undefined;

                                // For kind field restore kind value and all configured kinds.
                                if (field.model === 'kind') {
                                    var kind = getModel(backupItem, field)[field.model];

                                    var details = field.details;

                                    var keys = Object.keys(details);

                                    for (var detIx = 0; detIx < keys.length; detIx++)
                                        restoreFields(details[keys[detIx]].fields);
                                }
                            }
                        }
                        else if (field.type === 'panel-details')
                            restoreFields(field.details);
                    }
                }

                // Find group metadata to reset group values.
                for (var grpIx = 0; grpIx < groups.length; grpIx ++) {
                    if (groups[grpIx].group === group) {
                        var fields = groups[grpIx].fields;

                        restoreFields(fields);

                        break;
                    }
                }
            },
            formUI: function () {
                return {
                    ready: false,
                    expanded: false,
                    groups: [],
                    addGroups: function (general, advanced) {
                        if (general)
                            $.merge(this.groups, general);

                        if (advanced)
                            $.merge(this.groups, advanced);
                    },
                    isDirty: function () {
                        return _.findIndex(this.groups, function (group) {
                            return group.dirty;
                        }) >= 0;
                    },
                    markPristine: function () {
                        this.groups.forEach(function (group) {
                            group.dirty = false;
                        });
                    },
                    checkDirty: function(curItem, srcItem) {
                        this.groups.forEach(function(group) {
                            if (checkGroupDirty(group, curItem, srcItem))
                                dirty = true;
                        });
                    }
                };
            },
            getQueryVariable: function(name) {
                var attrs = window.location.search.substring(1).split("&");

                var attr = _.find(attrs, function(attr) {
                    return attr === name || (attr.indexOf('=') >= 0 && attr.substr(0, attr.indexOf('=')) === name);
                });

                if (!isDefined(attr))
                    return undefined;

                if (attr === name)
                    return true;

                return attr.substr(attr.indexOf('=') + 1);
            }
        };
    }]);

// Confirm popup service.
consoleModule.service('$confirm', function ($modal, $rootScope, $q) {
    var scope = $rootScope.$new();

    var deferred;

    var confirmModal = $modal({templateUrl: '/templates/confirm.html', scope: scope, placement: 'center', show: false});

    scope.confirmOk = function () {
        deferred.resolve(true);

        confirmModal.hide();
    };

    scope.confirmCancel = function () {
        deferred.reject('cancelled');

        confirmModal.hide();
    };

    confirmModal.confirm = function (content) {
        scope.content = content || 'Confirm?';

        deferred = $q.defer();

        confirmModal.show();

        return deferred.promise;
    };

    return confirmModal;
});

// Confirm change location.
consoleModule.service('$unsavedChangesGuard', function ($rootScope) {
    return {
        install: function ($scope) {
            $scope.$on("$destroy", function() {
                window.onbeforeunload = null;
            });

            var unbind = $rootScope.$on('$stateChangeStart', function(event, toState, toParams, fromState, fromParams) {
                if ($scope.ui && $scope.ui.isDirty()) {
                    if (!confirm('You have unsaved changes.\n\nAre you sure you want to discard them?')) {
                        event.preventDefault();
                    } else {
                        unbind();
                    }
                }
            });

            window.onbeforeunload = function(){
                return $scope.ui && $scope.ui.isDirty()
                    ? 'You have unsaved changes.\n\nAre you sure you want to discard them?'
                    : undefined;
            };
        }
    };
});

// Service for confirm or skip several steps.
consoleModule.service('$confirmBatch', function ($rootScope, $modal,  $q) {
    var scope = $rootScope.$new();

    var contentGenerator = function () {
        return 'No content';
    };

    var deferred;

    var stepConfirmModal = $modal({templateUrl: '/template/batch-confirm.html', scope: scope, placement: 'center', show: false});

    function _done(cancel) {
        if (cancel)
            deferred.reject('cancelled');
        else
            deferred.resolve();

        stepConfirmModal.hide();
    }

    var items = [];
    var curIx = 0;

    function _nextElement(skip) {
        items[curIx].skip = skip;

        curIx++;

        if (curIx < items.length)
            scope.batchConfirm.content = contentGenerator(items[curIx]);
        else
            _done();
    }

    scope.batchConfirm = {
        applyToAll: false,
        cancel: function () {
            _done(true);
        },
        skip: function () {
            if (this.applyToAll) {
                for (var i = curIx; i < items.length; i++)
                    items[i].skip = true;

                _done();
            }
            else
                _nextElement(true);
        },
        overwrite: function () {
            if (this.applyToAll)
                _done();
            else
                _nextElement(false);
        },
        reset: function (itemsToConfirm) {
            items = itemsToConfirm;
            curIx = 0;
            this.applyToAll = false;
            this.content = (items && items.length > 0) ? contentGenerator(items[0]) : undefined;
        }
    };

    /**
     * Show confirm all dialog.
     *
     * @param confirmMessageFx Function to generate a confirm message.
     * @param itemsToConfirm Array of element to process by confirm.
     */
    stepConfirmModal.confirm = function (confirmMessageFx, itemsToConfirm) {
        contentGenerator = confirmMessageFx;

        scope.batchConfirm.reset(itemsToConfirm);

        deferred = $q.defer();

        stepConfirmModal.show();

        return deferred.promise;
    };

    return stepConfirmModal;
});

// 'Clone' popup service.
consoleModule.service('$clone', function ($modal, $rootScope, $q) {
    var scope = $rootScope.$new();

    var deferred;

    scope.ok = function (newName) {
        deferred.resolve(newName);

        copyModal.hide();
    };

    var copyModal = $modal({templateUrl: '/templates/clone.html', scope: scope, placement: 'center', show: false});

    copyModal.confirm = function (oldName) {
        scope.newName = oldName + '(1)';

        deferred = $q.defer();

        copyModal.show();

        return deferred.promise;
    };

    return copyModal;
});

// Tables support service.
consoleModule.service('$table', ['$common', '$focus', function ($common, $focus) {
    function _swapSimpleItems(a, ix1, ix2) {
        var tmp = a[ix1];

        a[ix1] = a[ix2];
        a[ix2] = tmp;
    }

    function _model(item, field) {
        return $common.getModel(item, field);
    }

    var table = {name: 'none', editIndex: -1};

    function _tableReset() {
        table.name = 'none';
        table.editIndex = -1;

        $common.hidePopover();
    }

    function _tableState(name, editIndex) {
        table.name = name;
        table.editIndex = editIndex;
    }

    function _tableUI(field) {
        var ui = field.ui;

        return ui ? ui : field.type;
    }

    function _tableFocus(focusId, index) {
        $focus((index < 0 ? 'new' : 'cur') + focusId + (index >= 0 ? index : ''));
    }

    function _tableSimpleValue(filed, index) {
        return index < 0 ? filed.newValue : filed.curValue;
    }

    function _tablePairValue(filed, index) {
        return index < 0 ? {key: filed.newKey, value: filed.newValue} : {key: filed.curKey, value: filed.curValue};
    }

    function _tableStartEdit(item, field, index) {
        _tableState(field.model, index);

        var val = _model(item, field)[field.model][index];

        var ui = _tableUI(field);

        if (ui === 'table-simple') {
            field.curValue = val;

            _tableFocus(field.focusId, index);
        }
        else if (ui === 'table-pair') {
            field.curKey = val[field.keyName];
            field.curValue = val[field.valueName];

            _tableFocus('Key' + field.focusId, index);
        }
        else if (ui === 'table-db-fields') {
            field.curDatabaseFieldName = val.databaseFieldName;
            field.curDatabaseFieldType = val.databaseFieldType;
            field.curJavaFieldName = val.javaFieldName;
            field.curJavaFieldType = val.javaFieldType;

            _tableFocus('DatabaseFieldName' + field.focusId, index);
        }
        else if (ui === 'table-indexes') {
            field.curIndexName = val.name;
            field.curIndexType = val.indexType;
            field.curIndexFields = val.fields;

            _tableFocus(field.focusId, index);
        }
    }

    function _tableNewItem(field) {
        _tableState(field.model, -1);

        var ui = _tableUI(field);

        if (ui === 'table-simple') {
            field.newValue = null;

            _tableFocus(field.focusId, -1);
        }
        else if (ui === 'table-pair') {
            field.newKey = null;
            field.newValue = null;

            _tableFocus('Key' + field.focusId, -1);
        }
        else if (ui === 'table-db-fields') {
            field.newDatabaseFieldName = null;
            field.newDatabaseFieldType = null;
            field.newJavaFieldName = null;
            field.newJavaFieldType = null;

            _tableFocus('DatabaseFieldName' + field.focusId, -1);
        }
        else if (ui === 'table-indexes') {
            field.newIndexName = null;
            field.newIndexType = null;
            field.newIndexFields = null;

            _tableFocus(field.focusId, -1);
        }
        else if (ui === 'table-index-fields') {
            _tableFocus('FieldName' + (field.sorted ? 'S' : '') + field.indexIdx, -1);
        }
    }

    return {
        tableVisibleRow: function (rows, row) {
            return !row || !row._id || _.findIndex(rows, function(item) {return item._id === row._id;}) >= 0;
        },
        tableState: function (name, editIndex) {
            _tableState(name, editIndex);
        },
        tableReset: function () {
            _tableReset();
        },
        tableNewItem: _tableNewItem,
        tableNewItemActive: function (field) {
            return table.name === field.model && table.editIndex < 0;
        },
        tableEditing: function (field, index) {
            return table.name === field.model && table.editIndex === index;
        },
        tableStartEdit: _tableStartEdit,
        tableRemove: function (item, field, index) {
            _tableReset();

            _model(item, field)[field.model].splice(index, 1);
        },
        tableSimpleSave: function (valueValid, item, field, index) {
            var simpleValue = _tableSimpleValue(field, index);

            if (valueValid(item, field, simpleValue, index)) {
                _tableReset();

                if (index < 0) {
                    if (_model(item, field)[field.model])
                        _model(item, field)[field.model].push(simpleValue);
                    else
                        _model(item, field)[field.model] = [simpleValue];

                    _tableNewItem(field);
                }
                else {
                    var arr = _model(item, field)[field.model];

                    arr[index] = simpleValue;

                    if (index < arr.length - 1)
                        _tableStartEdit(item, field, index + 1);
                    else
                        _tableNewItem(field);
                }
            }
        },
        tableSimpleSaveVisible: function (field, index) {
            return !$common.isEmptyString(_tableSimpleValue(field, index));
        },
        tableSimpleUp: function (item, field, index) {
            _tableReset();

            _swapSimpleItems(_model(item, field)[field.model], index, index - 1);
        },
        tableSimpleDown: function (item, field, index) {
            _tableReset();

            _swapSimpleItems(_model(item, field)[field.model], index, index + 1);
        },
        tableSimpleDownVisible: function (item, field, index) {
            return index < _model(item, field)[field.model].length - 1;
        },
        tablePairValue: _tablePairValue,
        tablePairSave: function (pairValid, item, field, index) {
            if (pairValid(item, field, index)) {
                var pairValue = _tablePairValue(field, index);

                var pairModel = {};

                if (index < 0) {
                    pairModel[field.keyName] = pairValue.key;
                    pairModel[field.valueName] = pairValue.value;

                    if (item[field.model])
                        item[field.model].push(pairModel);
                    else
                        item[field.model] = [pairModel];

                    _tableNewItem(field);
                }
                else {
                    pairModel = item[field.model][index];

                    pairModel[field.keyName] = pairValue.key;
                    pairModel[field.valueName] = pairValue.value;

                    if (index < item[field.model].length - 1)
                        _tableStartEdit(item, field, index + 1);
                    else
                        _tableNewItem(field);
                }
            }
        },
        tablePairSaveVisible: function (field, index) {
            var pairValue = _tablePairValue(field, index);

            return !$common.isEmptyString(pairValue.key) && !$common.isEmptyString(pairValue.value);
        },
        tableFocusInvalidField: function (index, id) {
            _tableFocus(id, index);

            return false;
        },
        tableFieldId: function (index, id) {
            return (index < 0 ? 'new' : 'cur') + id + (index >= 0 ? index : '');
        }
    };
}]);

// Preview support service.
consoleModule.service('$preview', ['$timeout', '$interval', function ($timeout, $interval) {
    var Range = require('ace/range').Range;

    var prevContent = [];

    var animation = {editor: null, stage: 0, start: 0, stop: 0};

    function _clearSelection(editor) {
        _.forEach(editor.session.getMarkers(false), function (marker) {
            editor.session.removeMarker(marker.id);
        });
    }

    /**
     * Switch to next stage of animation.
     */
    function _animate() {
        animation.stage += animation.step;

        var stage = animation.stage;

        var editor = animation.editor;

        _clearSelection(editor);

        animation.selections.forEach(function (selection) {
            editor.session.addMarker(new Range(selection.start, 0, selection.stop, 0),
                'preview-highlight-' + stage, 'line', false);
        });

        if (stage === animation.finalStage) {
            editor.animatePromise = null;

            if (animation.clearOnFinal)
                _clearSelection(editor);
        }
    }

    /**
     * Show selections with animation.
     *
     * @param editor Editor to show selection.
     * @param selections Array of selection intervals.
     */
    function _fadeIn(editor, selections) {
        _fade(editor, selections, 1, 0, 10, false);
    }

    /**
     * Hide selections with animation.
     *
     * @param editor Editor to show selection.
     * @param selections Array of selection intervals.
     */
    function _fadeOut(editor, selections) {
        _fade(editor, selections, -1, 10, 0, true);
    }

    /**
     * Selection with animation.
     *
     * @param editor Editor to show selection animation.
     * @param selections Array of selection intervals.
     * @param step Step of animation (1 or -1).
     * @param startStage Start stage of animaiton.
     * @param finalStage Final stage of animation.
     * @param clearOnFinal Boolean flat to clear selection on animation finish.
     */
    function _fade(editor, selections, step, startStage, finalStage, clearOnFinal) {
        var promise = editor.animatePromise;

        if (promise) {
            $interval.cancel(promise);

            _clearSelection(editor);
        }

        animation = {editor: editor, step: step, stage: startStage, finalStage: finalStage, clearOnFinal: clearOnFinal, selections: selections};

        editor.animatePromise = $interval(_animate, 100, 10, false);
    }

    function previewChanged (ace) {
        var content = ace[0];

        var editor = ace[1];

        var clearPromise = editor.clearPromise;

        var newContent = content.lines;

        if (content.action === 'remove')
            prevContent = newContent;
        else if (prevContent.length > 0 && newContent.length > 0 && editor.attractAttention) {
            if (clearPromise) {
                $timeout.cancel(clearPromise);

                _clearSelection(editor);
            }

            var selections = [];

            var newIx = 0;
            var prevIx = 0;

            var prevLen = prevContent.length - (prevContent[prevContent.length - 1] === '' ? 1 : 0);
            var newLen = newContent.length - (newContent[newContent.length - 1] === '' ? 1 : 0);

            var removed = newLen < prevLen;

            var skipEnd = 0;

            var selected = false;
            var scrollTo = -1;

            while (newContent[newLen - 1] === prevContent[prevLen - 1] && newLen > 0 && prevLen > 0) {
                prevLen -= 1;
                newLen -= 1;

                skipEnd += 1;
            }

            while (newIx < newLen || prevIx < prevLen) {
                var start = -1;
                var end = -1;

                // Find an index of a first line with different text.
                for (; (newIx < newLen || prevIx < prevLen) && start < 0; newIx++, prevIx++) {
                    if (newIx >= newLen || prevIx >= prevLen || newContent[newIx] !== prevContent[prevIx]) {
                        start = newIx;

                        break;
                    }
                }

                if (start >= 0) {
                    // Find an index of a last line with different text by checking last string of old and new content in reverse order.
                    for (var i = start; i < newLen && end < 0; i ++) {
                        for (var j = prevIx; j < prevLen && end < 0; j ++) {
                            if (newContent[i] === prevContent[j] && newContent[i] !== '') {
                                end = i;

                                newIx = i;
                                prevIx = j;

                                break;
                            }
                        }
                    }

                    if (end < 0) {
                        end = newLen;

                        newIx = newLen;
                        prevIx = prevLen;
                    }

                    if (start === end) {
                        if (removed)
                            start = Math.max(0, start - 1);

                        end = Math.min(newLen + skipEnd, end + 1);
                    }

                    if (start <= end) {
                        selections.push({start: start, stop: end});

                        if (!selected)
                            scrollTo = start;

                        selected = true;
                    }
                }
            }

            // Run clear selection one time.
            if (selected) {
                _fadeIn(editor, selections);

                editor.clearPromise = $timeout(function () {
                    _fadeOut(editor, selections);

                    editor.clearPromise = null;
                }, 2000);

                editor.scrollToRow(scrollTo);
            }

            prevContent = [];
        }
        else
            editor.attractAttention = true;
    }

    return {
        previewInit: function (preview) {
            preview.setReadOnly(true);
            preview.setOption('highlightActiveLine', false);
            preview.setAutoScrollEditorIntoView(true);
            preview.$blockScrolling = Infinity;
            preview.attractAttention = false;

            var renderer = preview.renderer;

            renderer.setHighlightGutterLine(false);
            renderer.setShowPrintMargin(false);
            renderer.setOption('fontSize', '10px');
            renderer.setOption('maxLines', '50');

            preview.setTheme('ace/theme/chrome');
        },
        previewChanged: previewChanged
    };
}]);

consoleModule.service('ngCopy', ['$window', '$common', function ($window, $common) {
    var body = angular.element($window.document.body);

    var textArea = angular.element('<textarea/>');

    textArea.css({
        position: 'fixed',
        opacity: '0'
    });

    return function (toCopy) {
        textArea.val(toCopy);

        body.append(textArea);

        textArea[0].select();

        try {
            if (document.execCommand('copy'))
                $common.showInfo('Value copied to clipboard');
            else
                window.prompt("Copy to clipboard: Ctrl+C, Enter", toCopy);
        } catch (err) {
            window.prompt("Copy to clipboard: Ctrl+C, Enter", toCopy);
        }

        textArea.remove();
    };
}]).directive('ngClickCopy', ['ngCopy', function (ngCopy) {
    return {
        restrict: 'A',
        link: function (scope, element, attrs) {
            element.bind('click', function () {
                ngCopy(attrs.ngClickCopy);
            });

            if (!document.queryCommandSupported('copy'))
                element.hide();
        }
    };
}]);

consoleModule.filter('tablesSearch', function() {
    return function(array, query) {
        if (!angular.isUndefined(array) && !angular.isUndefined(query) && !angular.isUndefined(query.$)) {
            var filtredArray = [];

            var matchString = query.$.toLowerCase();

            angular.forEach(array, function (row) {
                var label = (row.schema + '.' + row.tbl).toLowerCase();

                if (label.indexOf(matchString) >= 0)
                    filtredArray.push(row);
            });

            return filtredArray;
        } else
            return array;
    };
});

// Filter metadata with key fields configuration.
consoleModule.filter('metadatasValidation', ['$common', function ($common) {
    return function(metadatas, valid, invalid) {
        if (valid && invalid)
            return metadatas;

        var out = [];

        _.forEach(metadatas, function (meta) {
            var _valid = !$common.metadataForStoreConfigured(meta) || $common.isJavaBuildInClass(meta.keyType) || !$common.isEmptyArray(meta.keyFields);

            if (valid && _valid || invalid && !_valid)
                out.push(meta);
        });

        return out;
    };
}]);

// Directive to enable validation for IP addresses.
consoleModule.directive('ipaddress', function () {
    const ip = '(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])';
    const port = '([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])';
    const portRange = '(:' + port + '(..' + port + ')?)?';
    const host = '(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])';

    return {
        require: 'ngModel',
        link: function (scope, elem, attrs, ctrl) {
            ctrl.$validators.ipaddress = function (modelValue, viewValue) {
                if (ctrl.$isEmpty(modelValue) || !attrs.ipaddress)
                    return true;

                return viewValue.match(new RegExp('(^' + ip + portRange + '$)|(^' + host + portRange + '$)')) != null;
            };
        }
    };
});

// Directive to enable validation to match specified value.
consoleModule.directive('match', function ($parse) {
    return {
        require: 'ngModel',
        link: function (scope, elem, attrs, ctrl) {
            scope.$watch(function () {
                return $parse(attrs.match)(scope) === ctrl.$modelValue;
            }, function (currentValue) {
                ctrl.$setValidity('mismatch', currentValue);
            });
        }
    };
});

// Directive to bind ENTER key press with some user action.
consoleModule.directive('onEnter', function ($timeout) {
    return function (scope, elem, attrs) {
        elem.on('keydown keypress', function (event) {
            if (event.which === 13) {
                scope.$apply(function () {
                    $timeout(function () {
                        scope.$eval(attrs.onEnter)
                    });
                });

                event.preventDefault();
            }
        });

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', function () {
            elem.off('keydown keypress');
        });
    };
});

// Directive to bind ESC key press with some user action.
consoleModule.directive('onEscape', function () {
    return function (scope, elem, attrs) {
        elem.on('keydown keypress', function (event) {
            if (event.which === 27) {
                scope.$apply(function () {
                    scope.$eval(attrs.onEscape);
                });

                event.preventDefault();
            }
        });

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', function () {
            elem.off('keydown keypress');
        });
    };
});

// Directive to retain selection. To fix angular-strap typeahead bug with setting cursor to the end of text.
consoleModule.directive('retainSelection', function ($timeout) {
    var promise;

    return function (scope, elem, attr) {
        elem.on('keydown', function (evt) {
            var key = evt.which;
            var ctrlDown = evt.ctrlKey || evt.metaKey;
            var input = this;
            var start = input.selectionStart;

            if (promise)
                $timeout.cancel(promise);

            promise = $timeout(function () {
                var setCursor = false;

                // Handle Backspace[8].
                if (key == 8 && start > 0) {
                    start -= 1;

                    setCursor = true;
                }
                // Handle Del[46].
                else if (key == 46)
                    setCursor = true;
                // Handle: Caps Lock[20], Tab[9], Shift[16], Ctrl[17], Alt[18], Esc[27], Enter[13], Arrows[37..40], Home[36], End[35], Ins[45], PgUp[33], PgDown[34], F1..F12[111..124], Num Lock[], Scroll Lock[145].
                else if (!(key == 8 || key == 9 || key == 13 || (key > 15 && key < 20) || key == 27 ||
                    (key > 32 && key < 41) || key == 45 || (key > 111 && key < 124) || key == 144 || key == 145)) {
                    // Handle: Ctrl + [A[65], C[67], V[86]].
                    if (!(ctrlDown && (key = 65 || key == 67 || key == 86))) {
                        start += 1;

                        setCursor = true;
                    }
                }

                if (setCursor)
                    input.setSelectionRange(start, start);

                promise = undefined;
            });
        });

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', function () {
            elem.off('keydown');
        });
    };
});

// Factory function to focus element.
consoleModule.factory('$focus', function ($timeout) {
    return function (id) {
        // Timeout makes sure that is invoked after any other event has been triggered.
        // E.g. click events that need to run before the focus or inputs elements that are
        // in a disabled state but are enabled when those events are triggered.
        $timeout(function () {
            var elem = $('#' + id);

            if (elem.length > 0) {
                var offset = elem.offset();

                var elemOffset = offset.top;

                var winOffset = window.pageYOffset;

                var topHeight = $('.padding-top-dflt.affix').outerHeight();

                if(elemOffset - 20 - topHeight < winOffset
                    || elemOffset + elem.outerHeight(true) + 20 > winOffset + window.innerHeight)
                    $('html, body').animate({
                        scrollTop: elemOffset - 20 - topHeight
                    }, 10);

                elem[0].focus();
            }
        }, 100);
    };
});

// Directive to auto-focus element.
consoleModule.directive('autoFocus', function($timeout) {
    return {
        restrict: 'AC',
        link: function(scope, element) {
            $timeout(function(){
                element[0].focus();
            });
        }
    };
});

// Directive to focus next element on ENTER key.
consoleModule.directive('enterFocusNext', function ($focus) {
    return function (scope, elem, attrs) {
        elem.on('keydown keypress', function (event) {
            if (event.which === 13) {
                event.preventDefault();

                $focus(attrs.enterFocusNext);
            }
        });
    };
});

// Directive to mark elements to focus.
consoleModule.directive('onClickFocus', function ($focus) {
    return function (scope, elem, attr) {
        elem.on('click', function () {
            $focus(attr.onClickFocus);
        });

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', function () {
            elem.off('click');
        });
    };
});

// Navigation bar controller.
consoleModule.controller('activeLink', [
    '$scope', function ($scope) {
        $scope.configurationDropdown = [
            {"text": "Clusters", "href": "/configuration/clusters"},
            {"text": "Caches", "href": "/configuration/caches"},
            {"text": "Metadata", "href": "/configuration/metadata"},
            {"text": "IGFS", "href": "/configuration/igfs"},
            {"text": "Summary", "href": "/configuration/summary"}
        ];

        $scope.isActive = function (path) {
            return new RegExp(path).test(window.location.pathname);
        };
    }]);

consoleModule.controller('resetPassword', [
    '$scope', '$modal', '$http', '$common', '$focus', 'Auth', '$state',
    function ($scope, $modal, $http, $common, $focus, Auth, $state) {
        if ($state.params.token)
            $http.post('/api/v1/validate/token', {token: $state.params.token})
                .success(function (res) {
                    $scope.email = res.email;
                    $scope.token = res.token;
                    $scope.error = res.error;

                    if ($scope.token && !$scope.error)
                        $focus('user_password');
                });

        // Try to reset user password for provided token.
        $scope.resetPassword = function (reset_info) {
            $http.post('/api/v1/password/reset', reset_info)
                .success(function () {
                    $common.showInfo('Password successfully changed');

                    $state.go('base.configuration.clusters');
                })
                .error(function (data, state) {
                    $common.showError(data);

                    if (state === 503)
                        $state.go('base.configuration.clusters');
                });
        };
    }
]);

// Login popup controller.
// TODO IGNITE-1936 Refactor this controller.
consoleModule.controller('auth', [
    '$scope', '$focus', 'Auth',
    function ($scope, $focus, Auth) {
        $scope.auth = Auth.auth;

        $scope.action = 'login';

        $focus('user_email');
    }]);

// Download agent controller.
consoleModule.controller('agent-download', [
    '$http', '$common', '$scope', '$interval', '$modal', '$state', function ($http, $common, $scope, $interval, $modal, $state) {
        // Pre-fetch modal dialogs.
        var _agentDownloadModal = $modal({scope: $scope, templateUrl: '/templates/agent-download.html', show: false, backdrop: 'static'});

        var _agentDownloadHide = _agentDownloadModal.hide;

        /**
         * Special dialog hide function.
         */
        _agentDownloadModal.hide = function () {
            $common.hideAlert();

            _agentDownloadHide();
        };

        /**
         * Close dialog and go by specified link.
         */
        $scope.goBack = function () {
            _stopInterval();

            _agentDownloadModal.hide();

            if (_agentDownloadModal.backLink)
                $state.go(_agentDownloadModal.backLink);
        };

        $scope.downloadAgent = function () {
            var lnk = document.createElement('a');

            lnk.setAttribute('href', '/api/v1/agent/download/zip');
            lnk.setAttribute('target', '_self');
            lnk.style.display = 'none';

            document.body.appendChild(lnk);

            lnk.click();

            document.body.removeChild(lnk);
        };

        /**
         * Base handler of exceptions on agent interaction
         *
         * @param errMsg Error message.
         * @param status Error code.
         * @param timedOut True if request timedOut.
         */
        var _handleException = function (errMsg, status, timedOut) {
            if (_agentDownloadModal.skipSingleError)
                _agentDownloadModal.skipSingleError = false;
            else if (!_agentDownloadModal.$isShown)
                _agentDownloadModal.$promise.then(_agentDownloadModal.show);

            $scope.nodeFailedConnection = status === 404 || timedOut;

            if (status === 500)
                $common.showError(errMsg, 'top-right', 'body', true);
        };

        /**
         * Start interval to agent listening.
         */
        function _startInterval() {
            _agentDownloadModal.updatePromise = $interval(function () {
                _tryWithAgent();
            }, 5000, 0, false);

            _tryWithAgent();
        }

        /**
         * Stop interval to agent listening.
         */
        function _stopInterval() {
            $interval.cancel(_agentDownloadModal.updatePromise);
        }

        /**
         * Try to access agent and execute specified function.
         */
        function _tryWithAgent() {
            var timeout = 3000,
                timedOut = false;

            setTimeout(function () {
                timedOut = true;
            }, timeout);

            $http.post(_agentDownloadModal.checkUrl, undefined, {timeout: timeout})
                .success(function (result) {
                    _agentDownloadModal.skipSingleError = true;

                    _agentDownloadModal.hide();

                    if (_agentDownloadModal.awaitFirstSuccess)
                        _stopInterval();

                    _agentDownloadModal.checkFn(result, _agentDownloadModal.hide, _handleException);
                })
                .error(function (errMsg, status) {
                    _handleException(errMsg, status, timedOut);
                });
        }

        /**
         * Start awaiting agent start using ping.
         *
         * @param checkFn Function to execute by timer when agent available.
         */
        $scope.awaitAgent = function (checkFn) {
            _agentDownloadModal.skipSingleError = false;

            _agentDownloadModal.checkUrl = '/api/v1/agent/ping';

            _agentDownloadModal.checkFn = checkFn;

            // Stop refresh after first success.
            _agentDownloadModal.awaitFirstSuccess = true;

            $scope.agentDownloadBackTo = 'Metadata';

            _startInterval();
        };

        /**
         * Start listening topology from node.
         *
         * @param checkFn Function to execute by timer when agent available.
         */
        $scope.startTopologyListening = function (checkFn) {
            _agentDownloadModal.skipSingleError = false;

            _agentDownloadModal.checkUrl = '/api/v1/agent/topology';

            _agentDownloadModal.checkFn = checkFn;

            _agentDownloadModal.backLink = 'base.configuration.clusters';

            $scope.agentDownloadBackTo = 'Configuration';

            _startInterval();
        };

        /**
         * Stop listening of agent by ping.
         */
        $scope.finishAgentListening = function () {
            _stopInterval();
        };
    }]);

// Navigation bar controller.
consoleModule.controller('notebooks', ['$scope', '$modal', '$state', '$http', '$common',
    function ($scope, $modal, $state, $http, $common) {
    $scope.$root.notebooks = [];

    // Pre-fetch modal dialogs.
    var _notebookNewModal = $modal({scope: $scope, templateUrl: '/sql/notebook-new.html', show: false});

    $scope.$root.rebuildDropdown = function() {
        $scope.notebookDropdown = [
            {text: 'Create new notebook', click: 'inputNotebookName()'},
            {divider: true}
        ];

        _.forEach($scope.$root.notebooks, function (notebook) {
            $scope.notebookDropdown.push({
                text: notebook.name,
                href: '/sql/' + notebook._id
            });
        });
    };

    $scope.$root.reloadNotebooks = function() {
        // When landing on the page, get clusters and show them.
        $http.post('/api/v1/notebooks/list')
            .success(function (data) {
                $scope.$root.notebooks = data;

                $scope.$root.rebuildDropdown();
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.$root.inputNotebookName = function() {
        _notebookNewModal.$promise.then(_notebookNewModal.show);
    };

    $scope.$root.createNewNotebook = function(name) {
        $http.post('/api/v1/notebooks/new', {name: name})
            .success(function (id) {
                _notebookNewModal.hide();

                $scope.$root.reloadNotebooks();

                $state.go('base.sql', {id: id});
            })
            .error(function (message, state) {
                $common.showError(message);
            });
    };

    $scope.$root.reloadNotebooks();
}]);

// Navigation bar controller.
consoleModule.controller('save-remove', ['$scope', function ($scope) {
    $scope.removeDropdown = [{ 'text': 'Remove All', 'click': 'removeAllItems()'}];
}]);
