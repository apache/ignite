
/**
 * Adds an event listener on "Import" button.
 * 
 * @returns {void}
 */
MPG.eventListeners.addImport = function() {

    document.querySelector('#mpg-import-button').addEventListener('click', function(event) {

        event.preventDefault();

        if ( MPG.databaseName === '' || MPG.collectionName === '' ) {
            return window.alert('Please select a database and a collection.');
        }

        if ( document.querySelector('#mpg-import-file').files.length === 0 ) {
            return window.alert('Please select a file on your computer.');
        }

        document.querySelector('input[name="database_name"]').value = MPG.databaseName;
        document.querySelector('input[name="collection_name"]').value = MPG.collectionName;

        document.querySelector('#mpg-import-form').submit();

    });

};

// When document is ready:
window.addEventListener('DOMContentLoaded', function(_event) {

    MPG.eventListeners.addMenuToggle();
    MPG.eventListeners.addDatabases();
    MPG.eventListeners.addImport();

    MPG.helpers.navigateOnSamePage();

});
