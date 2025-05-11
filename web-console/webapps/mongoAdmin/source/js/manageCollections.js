
/**
 * Adds an event listener on "Create coll." button.
 * 
 * @returns {void}
 */
MPG.eventListeners.addCreateColl = function() {

    document.querySelector('#mpg-create-coll-button').addEventListener('click', function(_event) {
		
        var databaseName = MPG.databaseName;
        if ( databaseName === null ) {
            window.alert('Database name to select and use');
			return ;
        }
        
        var collectionName = window.prompt('Collection name to create');
        if ( collectionName === null ) {
            return;
        }
    
        var requestBody = {
            'databaseName': databaseName,
            'collectionName': collectionName
        };
    
        MPG.helpers.doAjaxRequest(
            'POST',
            './createCollection',
            function(response) {

                if ( JSON.parse(response) === true ) {
                    window.location.hash = '#';
                    window.location.reload();
                }

            },
            JSON.stringify(requestBody)
        );

    });

};

/**
 * Adds an event listener on "Rename coll." button.
 * 
 * @returns {void}
 */
MPG.eventListeners.addRenameColl = function() {

    document.querySelector('#mpg-rename-coll-button').addEventListener('click', function(_event) {

        if ( MPG.databaseName === '' || MPG.collectionName === '' ) {
            return window.alert('Please select a database and a collection.');
        }

        var newCollectionName = window.prompt('New collection name');
        if ( newCollectionName === null ) {
            return;
        }

        var requestBody = {
            'databaseName': MPG.databaseName,
            'oldCollectionName': MPG.collectionName,
            'newCollectionName': newCollectionName
        };

        MPG.helpers.doAjaxRequest(
            'POST',
            './renameCollection',
            function(response) {

                if ( JSON.parse(response) === true ) {
                    window.location.hash = '#' + MPG.databaseName;
                    MPG.helpers.completeNavLinks(window.location.hash);
                    MPG.reloadCollections(MPG.databaseName);
                }

            },
            JSON.stringify(requestBody)
        );

    });

};

/**
 * Adds an event listener on "Drop coll." button.
 * 
 * @returns {void}
 */
MPG.eventListeners.addDropColl = function() {

    document.querySelector('#mpg-drop-coll-button').addEventListener('click', function(_event) {

        if ( MPG.databaseName === '' || MPG.collectionName === '' ) {
            return window.alert('Please select a database and a collection.');
        }

        var dropConfirmation = window.confirm(
            'Do you REALLY want to DROP collection: '
                + MPG.databaseName + '.' + MPG.collectionName
        );

        if ( dropConfirmation === false ) {
            return;
        }

        var requestBody = {
            'databaseName': MPG.databaseName,
            'collectionName': MPG.collectionName
        };

        MPG.helpers.doAjaxRequest(
            'POST',
            './dropCollection',
            function(response) {

                if ( JSON.parse(response) === true ) {
                    window.location.hash = '#';
                    window.location.reload();
                }

            },
            JSON.stringify(requestBody)
        );

    });

};


/**
 * Adds an event listener on "Drop coll." button.
 * 
 * @returns {void}
 */
MPG.eventListeners.addCollStats = function() {

    document.querySelector('#mpg-stats-coll-button').addEventListener('click', function(_event) {

        if ( MPG.databaseName === '' || MPG.collectionName === '' ) {
            return window.alert('Please select a database and a collection.');
        }
        

        var requestBody = {
            'databaseName': MPG.databaseName,
            'collectionName': MPG.collectionName
        };

        MPG.helpers.doAjaxRequest(
            'POST',
            './collStats',
            function(response) {

                if (response) {
                   var result = document.getElementById('collectionStats');
                   result.innerHTML= response;
                }

            },
            JSON.stringify(requestBody)
        );

    });

};

// When document is ready:
window.addEventListener('DOMContentLoaded', function(_event) {

    MPG.eventListeners.addMenuToggle();
    MPG.eventListeners.addDatabases();
    MPG.eventListeners.addCreateColl();
    MPG.eventListeners.addRenameColl();
    MPG.eventListeners.addDropColl();
    MPG.eventListeners.addCollStats();

    MPG.helpers.navigateOnSamePage();

});
