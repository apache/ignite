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

// TODO: Refactor this service for legacy tables with more than one input field.
export default ['IgniteLegacyUtils', [
    '$alert', '$popover', '$anchorScroll', '$location', '$timeout', '$window', 'IgniteFocus',
    ($alert, $popover, $anchorScroll, $location, $timeout, $window, Focus) => {
        $anchorScroll.yOffset = 55;

        function isDefined(v) {
            return !_.isNil(v);
        }

        function isEmptyString(s) {
            if (isDefined(s))
                return s.trim().length === 0;

            return true;
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

                $timeout(() => newPopover.$promise.then(() => {
                    newPopover.show();

                    // Workaround to fix popover location when content is longer than content template.
                    // https://github.com/mgcrea/angular-strap/issues/1497
                    $timeout(newPopover.$applyPlacement);
                }), 400);
                $timeout(() => newPopover.hide(), showTime || 5000);
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
                    Focus.move(focusId);
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

        function compareSQLSchemaNames(firstCache, secondCache) {
            const firstName = firstCache.sqlSchema;
            const secondName = secondCache.sqlSchema;

            if (firstName && secondName && (firstName === secondName))
                return {checked: false, firstCache, secondCache};

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
                        if (checkCacheExt._id !== curCache._id) {
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
            checkCacheSQLSchemas(caches, checkCacheExt) {
                let res = DS_CHECK_SUCCESS;

                _.find(caches, (curCache, curIx) => {
                    if (isDefined(checkCacheExt)) {
                        if (checkCacheExt._id !== curCache._id) {
                            res = compareSQLSchemaNames(checkCacheExt, curCache);

                            return !res.checked;
                        }

                        return false;
                    }

                    return _.find(caches, function(checkCache, checkIx) {
                        if (checkIx < curIx) {
                            res = compareSQLSchemaNames(checkCache, curCache);

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
                            },
                            CacheJdbcBlobStoreFactory: { connectVia: 'DataSource' }
                        },
                        readThrough: dflt || cache.readThrough,
                        writeThrough: dflt || cache.writeThrough
                    };
                }
            },
            autoClusterSwapSpiConfiguration(cluster, caches) {
                const swapConfigured = cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind;

                if (!swapConfigured && _.find(caches, (cache) => cache.swapEnabled))
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
            },
            checkFieldValidators(ui) {
                const form = ui.inputForm;
                const errors = form.$error;
                const errKeys = Object.keys(errors);

                if (errKeys && errKeys.length > 0) {
                    const firstErrorKey = errKeys[0];

                    const firstError = errors[firstErrorKey][0];
                    const actualError = firstError.$error[firstErrorKey][0];

                    const errNameFull = actualError.$name;
                    const errNameShort = errNameFull.endsWith('TextInput') ? errNameFull.substring(0, errNameFull.length - 9) : errNameFull;

                    const extractErrorMessage = function(errName) {
                        try {
                            return errors[firstErrorKey][0].$errorMessages[errName][firstErrorKey];
                        }
                        catch (ignored) {
                            try {
                                return form[firstError.$name].$errorMessages[errName][firstErrorKey];
                            }
                            catch (ignited) {
                                return false;
                            }
                        }
                    };

                    const msg = extractErrorMessage(errNameFull) || extractErrorMessage(errNameShort) || 'Invalid value!';

                    return showPopoverMessage(ui, firstError.$name, errNameFull, msg);
                }

                return true;
            }
        };
    }
]];
