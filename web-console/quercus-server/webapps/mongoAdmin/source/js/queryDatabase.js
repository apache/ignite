
/**
 * Instance of CodeMirror.
 * 
 * @type {?CodeMirror}
 */
MPG.codeMirror = null;

/**
 * History of queries.
 * 
 * @type {Array}
 */
MPG.queryHistory = [];

/**
 * List of MongoDB and SQL keywords.
 * XXX Used for autocompletion.
 * 
 * @type {Array}
 */
MPG.mongoDBAndSQLKeywords = [

    '$eq', '$gt', '$gte', '$in', '$lt', '$lte', '$ne', '$nin',
    '$and', '$not', '$nor', '$or', '$exists', '$type',

    'SELECT', 'FROM', 'WHERE', 'IN', 'LIKE', 'AND', 'NOT', 'OR'

];

/**
 * Field names of current collection.
 * 
 * @type {Array}
 */
MPG.collectionFields = [];

/**
 * Document ID.
 * XXX Used by JsonView parser.
 * 
 * @type {string}
 */
MPG.documentId = '';

/**
 * Cached output.
 * 
 * @type {string}
 */
MPG.cachedOutput = '';

/**
 * Initializes CodeMirror instance.
 * 
 * @returns {void}
 */
MPG.initializeCodeMirror = function() {

    MPG.codeMirror = CodeMirror.fromTextArea(
        document.querySelector('#mpg-filter-or-doc-textarea')
    );

    var historyPopButton = document.createElement('button');
    
    historyPopButton.className = 'mpg-history-pop-button';
    historyPopButton.innerHTML = '<i class="fa fa-history" aria-hidden="true"></i>';

    document.querySelector('.CodeMirror').appendChild(historyPopButton);

};

/**
 * Indicates if device is extra small.
 * 
 * @returns {boolean}
 */
MPG.helpers.isXsDevice = function() {

    return window.matchMedia('(max-width: 576px)').matches;

};

/**
 * Converts a SQL query to a MongoDB query.
 * 
 * @param {string} sql
 * @param {function} successCallback
 * 
 * @returns {void}
 */
MPG.helpers.convertSQLToMongoDBQuery = function(sql, successCallback) {

    if ( /GROUP BY/i.test(sql) ) {
        MPG.codeMirror.setValue(sql);
        return window.alert('SQL GROUP BY clause is not supported.');
    }

    if ( /ORDER BY/i.test(sql) ) {
        MPG.codeMirror.setValue(sql);
        return window.alert('SQL ORDER BY clause is not supported.');
    }

    if ( /LIMIT/i.test(sql) ) {
        MPG.codeMirror.setValue(sql);
        return window.alert('SQL LIMIT clause is not supported.');
    }

    MPG.helpers.doAjaxRequest(
        'POST',
        './convertSQLToMongoDBQuery',
        successCallback,
        JSON.stringify({ "sql": sql })
    );

};

/**
 * Converts a string to any type.
 * 
 * @param {string} string 
 * @param {string} targetType
 * 
 * @returns {*}
 * 
 * @throws {Error}
 */
MPG.helpers.convertStringToAny = function(string, targetType) {

    var castedString = string;

    switch (targetType) {

        case 'number':
            castedString = ( string.indexOf('.') !== -1 ) ? parseFloat(string) : parseInt(string);
            if ( isNaN(castedString) ) {
                throw Error('[MongoDB PHP GUI] "' + string + '" is not a number');
            }
            break;

        case 'boolean':
            if ( string === 'true' ) {
                castedString = true;
            } else if ( string === 'false' ) {
                castedString = false;
            } else {
                throw Error('[MongoDB PHP GUI] "' + string + '" is not a boolean');
            }
            break;

        case 'object':
            castedString = ( string === 'null' ) ? null : JSON.parse(string);
            break;

    }

    return castedString;

};

/**
 * Converts any type to a string.
 * 
 * @param {*} any
 * 
 * @returns {*}
 */
MPG.helpers.convertAnyToString = function(any) {

    var string;

    switch (typeof any) {

        case 'object':
            string = ( any === null ) ? 'null' : JSON.stringify(any);
            break;

        case 'number':
        case 'boolean':
        default:
            string = any.toString();
            break;

    }

    return string;

};

/**
 * Downloads a file.
 * 
 * @see https://stackoverflow.com/questions/3665115/how-to-create-a-file-in-memory-for-user-to-download-but-not-through-server
 * 
 * @param {string} filename
 * @param {string} data
 * @param {string} type
 * 
 * @returns {void}
 */
MPG.helpers.downloadFile = function(filename, data, type) {

    var blob = new Blob([data], {type: type});

    if ( window.navigator.msSaveOrOpenBlob ) {

        window.navigator.msSaveBlob(blob, filename);

    } else {

        var elem = window.document.createElement('a');
        elem.href = window.URL.createObjectURL(blob);
        elem.download = filename;
        document.body.appendChild(elem);
        elem.click();
        document.body.removeChild(elem);

    }

};

/**
 * Fixes responsive design.
 * 
 * @returns {void}
 */
MPG.fixResponsiveDesign = function() {

    if ( MPG.helpers.isXsDevice() ) {

        document.querySelector('#mpg-insert-one-button').innerHTML = 'Insert';
        document.querySelector('#mpg-delete-one-button').innerHTML = 'Delete';

    }

};

/**
 * Adds an event listener on each collection.
 * 
 * @returns {void}
 */
MPG.eventListeners.addCollections = function() {

    document.querySelectorAll('.mpg-collection-link').forEach(function(collectionLink) {

        collectionLink.addEventListener('click', function(_event) {
            
            MPG.collectionName = collectionLink.dataset.collectionName;
            MPG.helpers.completeNavLinks('#' + MPG.databaseName + '/' + MPG.collectionName);
            
            document.querySelectorAll('.mpg-collection-link').forEach(function(collectionLink) {
                collectionLink.classList.remove('active');
            });

            collectionLink.classList.add('active');

            MPG.collectionFields = [];

            var requestBody = {
                'databaseName': MPG.databaseName,
                'collectionName': MPG.collectionName
            };

            MPG.helpers.doAjaxRequest(
                'POST',
                './enumCollectionFields',
                function(response) {

                    JSON.parse(response).forEach(function(collectionField) {
                        if ( typeof collectionField === 'string' ) {
                            MPG.collectionFields.push(collectionField);
                        }
                    });

                    var sortSelect = document.querySelector('#mpg-sort-select');
                    sortSelect.innerHTML = '<option value=""></option>';

                    MPG.collectionFields.forEach(function(collectionField) {

                        sortSelect.innerHTML += '<option value="' + collectionField + '">'
                            + collectionField + '</option>';

                    });

                    document.querySelector('#mpg-output-code').innerHTML = '';

                    document.querySelector('#mpg-find-button').click();

                },
                JSON.stringify(requestBody)
            );

        });

    });

    if ( MPG.toDoList.reselectCollection !== '' ) {

        var collectionSelector = '.mpg-collection-link' + '[data-collection-name="'
            + MPG.toDoList.reselectCollection + '"]';

        var collection = document.querySelector(collectionSelector);

        if ( collection ) {
            collection.click();
        } else {
            window.alert('Error: Collection not found. Select another one.');
            window.location.hash = '';
        }

        MPG.toDoList.reselectCollection = '';

    }

};

/**
 * Adds an event listener on "Insert one" button.
 * 
 * @returns {void}
 */
MPG.eventListeners.addRunCommand = function() {

    document.querySelector('#mpg-run-command-button').addEventListener('click', function(_event) {

        if ( MPG.databaseName === '' ) {
            return window.alert('Please select a database and a collection.');
        }

        // Synchronizes CodeMirror with Filter or Document text area.
        MPG.codeMirror.save();

        var requestBody = {
            'databaseName': MPG.databaseName,
            'collectionName': MPG.collectionName
        };
        
        var filterOrDocTextAreaValue = document.querySelector('#mpg-filter-or-doc-textarea').value;

        if ( filterOrDocTextAreaValue === '' ) {
            return window.alert('Please fill the document text area.');
        }
        
        requestBody.filter = jsonic(filterOrDocTextAreaValue);
        
        MPG.helpers.doAjaxRequest(
            'POST',
            './runCommand',
            function(response) {

                MPG.cachedOutput = '';

                var outputCode = document.querySelector('#mpg-output-code');
                outputCode.innerHTML = '';

                var jsonViewTree = JsonView.createTree(response);
                JsonView.render(jsonViewTree, outputCode);
                document.querySelector('.json-key').textContent = 'inserted';
                JsonView.expandChildren(jsonViewTree);

            },
            JSON.stringify(requestBody)
        );

    });

};

/**
 * Adds an event listener on "Count" button.
 * 
 * @returns {void}
 */
MPG.eventListeners.addStats = function() {

    document.querySelector('#mpg-stats-button').addEventListener('click', function(_event) {

        if ( MPG.databaseName === '' ) {
            return window.alert('Please select a database.');
        }        

        var requestBody = {
            'databaseName': MPG.databaseName,
            'collectionName': MPG.collectionName
        };
        

        MPG.helpers.doAjaxRequest(
            'POST',
            './dbstats',
            function(response) {

                MPG.cachedOutput = '';
                
                var outputCode = document.querySelector('#mpg-output-code');
                outputCode.innerHTML = '';

                var outputStats = document.querySelector('#dbStats');
                outputStats.innerHTML = response;
            },
            JSON.stringify(requestBody)
        );

    });

};

/**
 * Adds an event listener on "Delete one" button.
 * 
 * @returns {void}
 */
MPG.eventListeners.addDeleteOne = function() {

    document.querySelector('#mpg-delete-one-button').addEventListener('click', function(_event) {

        if ( MPG.databaseName === '' || MPG.collectionName === '' ) {
            return window.alert('Please select a database and a collection.');
        }

        // Synchronizes CodeMirror with Filter or Document text area.
        MPG.codeMirror.save();

        var requestBody = {
            'databaseName': MPG.databaseName,
            'collectionName': MPG.collectionName
        };

        var filterOrDocTextAreaValue = document.querySelector('#mpg-filter-or-doc-textarea').value;

        if ( filterOrDocTextAreaValue === '' ) {
            return window.alert('Please fill the filter text area.');
        }

        var deleteConfirmation = window.confirm(
            'Do you really want to delete document matching this criteria:\n' + filterOrDocTextAreaValue
        )

        if ( deleteConfirmation === false ) {
            return;
        }

        requestBody.filter = jsonic(filterOrDocTextAreaValue);
        
        MPG.helpers.doAjaxRequest(
            'POST',
            './deleteOneDocument',
            function(response) {

                MPG.cachedOutput = '';

                var outputCode = document.querySelector('#mpg-output-code');
                outputCode.innerHTML = '';

                var jsonViewTree = JsonView.createTree(response);
                JsonView.render(jsonViewTree, outputCode);
                document.querySelector('.json-key').textContent = 'deleted';
                JsonView.expandChildren(jsonViewTree);

            },
            JSON.stringify(requestBody)
        );

    });

};

/**
 * Adds an event listener for updates.
 * 
 * @returns {void}
 */
MPG.eventListeners.addUpdate = function() {

    var updatableJsonValues = document.querySelectorAll(
        '.json-value[data-document-field-is-updatable="true"]'
    );

    updatableJsonValues.forEach(function(updatableJsonValue) {

        updatableJsonValue.addEventListener('click', function(event) {

            var documentField = event.currentTarget;

            var documentFieldNewValue = window.prompt(
                'New value',
                MPG.helpers.unescapeHTML(documentField.innerHTML)
            );

            if ( documentFieldNewValue === null ) {
                return;
            }

            documentFieldNewValue = MPG.helpers.convertStringToAny(
                documentFieldNewValue, documentField.dataset.documentFieldType
            );

            var documentId = documentField.dataset.documentId;

            var requestBody = {
                'databaseName': MPG.databaseName,
                'collectionName': MPG.collectionName,
                "filter": {
                    "_id": documentId
                },
                "update": {
                    "$set": {}
                }
            };

            requestBody.update.$set[documentField.dataset.documentFieldName] = documentFieldNewValue;

            MPG.helpers.doAjaxRequest(
                'POST',
                './updateOneDocument',
                function(response) {

                    if ( JSON.parse(response) === 1 ) {
                        documentField.innerHTML = MPG.helpers.escapeHTML(
                            MPG.helpers.convertAnyToString(documentFieldNewValue)
                        );
                    }

                },
                JSON.stringify(requestBody)
            );

        });

    })

};

/**
 * Adds an event listener on "CodeMirror" change.
 * 
 * @returns {void}
 */
MPG.eventListeners.addCodeMirror = function() {

    MPG.codeMirror.on('change', function() {

        // If Filter or Document text area contains SQL:
        if ( /^SELECT/i.test(MPG.codeMirror.getValue()) ) {
            MPG.codeMirror.setOption('mode', 'sql');
        } else {
            MPG.codeMirror.setOption('mode', 'javascript');
        }

    });
    
};

/**
 * Adds an event listener on "History pop" button.
 * 
 * @returns {void}
 */
MPG.eventListeners.addHistoryPop = function() {

    document.querySelector('.CodeMirror .mpg-history-pop-button')
        .addEventListener('click', function(_event) {

        var lastQuery = MPG.queryHistory.pop();

        if ( lastQuery === MPG.codeMirror.getValue() ) {
            lastQuery = MPG.queryHistory.pop();
        }

        if ( typeof lastQuery === 'string' ) {
            MPG.codeMirror.setValue(lastQuery);
            MPG.codeMirror.save();
        } else {
            MPG.codeMirror.setValue('');
            MPG.codeMirror.save();
        }

    });

};

/**
 * Adds an event listener on "Find" button.
 * 
 * @returns {void}
 */
MPG.eventListeners.addAggregation = function() {

    document.querySelector('#mpg-find-button').addEventListener('click', function(_event) {

        if ( MPG.databaseName === '' ) {
            return window.alert('Please select a database and a collection.');
        }

        // Synchronizes CodeMirror with Filter or Document text area.
        MPG.codeMirror.save();

        var filterOrDocTextAreaValue = document.querySelector('#mpg-filter-or-doc-textarea').value;

        // If Filter or Document text area contains SQL:
        if ( /^SELECT/i.test(filterOrDocTextAreaValue) ) {

            MPG.codeMirror.setValue('');
            MPG.codeMirror.save();
            
            return MPG.helpers.convertSQLToMongoDBQuery(filterOrDocTextAreaValue,
                function(response) {

                    MPG.codeMirror.setValue(response);
                    MPG.codeMirror.save();

                    document.querySelector('#mpg-find-button').click();

                }
            );

        }

        var requestBody = {
            'databaseName': MPG.databaseName,
            'collectionName': MPG.collectionName
        };

        if ( filterOrDocTextAreaValue === '' ) {
            requestBody.filter = {};
        } else {
            MPG.queryHistory.push(filterOrDocTextAreaValue);
            requestBody.filter = jsonic(filterOrDocTextAreaValue);
        }

        requestBody.options = {};
        requestBody.options.limit = parseInt(document.querySelector('#mpg-limit-input').value);        
        requestBody.options.skip = parseInt(document.querySelector('#mpg-skip-input').value);

        var sortSelect = document.querySelector('#mpg-sort-select');

        if ( sortSelect.value !== '' ) {

            var order = parseInt(document.querySelector('#mpg-order-select').value);

            requestBody.options.sort = {};
            requestBody.options.sort[sortSelect.value] = order;

        }

        MPG.helpers.doAjaxRequest(
            'POST',
            './aggregation',
            function(response) {

                MPG.cachedOutput = response;

                var outputCode = document.querySelector('#mpg-output-code');
                outputCode.innerHTML = '';

                var jsonViewTree = JsonView.createTree(response);
                JsonView.render(jsonViewTree, outputCode);
                document.querySelector('.json-key').textContent = 'documents';

                var foundDocuments = JSON.parse(response).length;

                if ( foundDocuments === 0 ) {

                    MPG.cachedOutput = '';
                    document.querySelector('.json-value').textContent = 'none';

                } else if ( foundDocuments <= 100 ) {

                    JsonView.expandChildren(jsonViewTree);

                } else {
                    window.alert('To preserve performances, output docs were collapsed.');
                }

                MPG.documentId = '';

                MPG.eventListeners.addUpdate();

            },
            JSON.stringify(requestBody)
        );

    });

};

/**
 * Adds an event listener for autocompletion/find key shortcuts.
 * 
 * @returns {void}
 */
MPG.eventListeners.addKeyShortcuts = function() {

    document.addEventListener('keydown', function(event) {

        if ( event.ctrlKey ) {

            switch (event.key) {

                case ' ':
                case 'Spacebar': // For old browsers.
                    MPG.codeMirror.showHint();
                    break;

                case 'Enter':
                    var findButton = document.querySelector('#mpg-find-button');
                    findButton.focus();
                    findButton.click();
                    break;

                case '*':
                    var countButton = document.querySelector('#mpg-count-button');
                    countButton.focus();
                    countButton.click();
                    break;

            }

        }

    });

};

/**
 * Adds an event listener on "Export" button.
 * 
 * @returns {void}
 */
MPG.eventListeners.addExport = function() {

    document.querySelector('#mpg-export-button').addEventListener('click', function(_event) {

        if ( MPG.cachedOutput === '' ) {
            return window.alert('There is nothing to export for now...');
        }

        MPG.helpers.downloadFile(
            'mongodb-php-gui-export-' + (new Date()).getTime() + '.json',
            JSON.stringify(JSON.parse(MPG.cachedOutput), null, 4),
            'application/json'
        );

    });

};

// When document is ready:
window.addEventListener('DOMContentLoaded', function(_event) {

    MPG.initializeCodeMirror();
    MPG.fixResponsiveDesign();

    MPG.eventListeners.addMenuToggle();
    MPG.eventListeners.addDatabases();
    MPG.eventListeners.addRunCommand();
    MPG.eventListeners.addStats();
    MPG.eventListeners.addDeleteOne();
    MPG.eventListeners.addCodeMirror();
    MPG.eventListeners.addHistoryPop();
    MPG.eventListeners.addAggregation();
    MPG.eventListeners.addKeyShortcuts();
    MPG.eventListeners.addExport();

    MPG.helpers.navigateOnSamePage();

});
