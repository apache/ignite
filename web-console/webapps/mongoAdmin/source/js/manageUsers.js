
/**
 * Reloads users of a specific database.
 * 
 * @param {string} databaseName
 * 
 * @returns {void}
 */
MPG.reloadUsers = function(databaseName) {

    var requestBody = { 'databaseName': databaseName };

    MPG.helpers.doAjaxRequest(
        'POST', './listUsers', function(response) {

            var usersInfo = JSON.parse(response);

            document.querySelector('#mpg-users-table').classList.add('d-none');

            var usersTableBody = document.querySelector('#mpg-users-table tbody');
            usersTableBody.innerHTML = '';

            if ( usersInfo.users.length === 0 ) {
                return;
            }

            usersInfo.users.forEach(function(userInfo) {

                var userRoles = [];

                userInfo.roles.forEach(function(userRole) {
                    userRoles.push(userRole.role + ' (' + userRole.db + ')');
                });

                var userDropButton = '<button'
                + ' data-user-name="' + userInfo.user + '"'
                    + ' class="mpg-drop-user-button btn btn-danger">'
                        + 'Drop user</button>';

                usersTableBody.innerHTML += '<tr><td>' + userInfo.user + '</td>'
                    + '<td>' + userRoles.join(', ') + '</td>'
                        + '<td>' + userDropButton + '</td></tr>';

            });

            MPG.eventListeners.addDropUser();

            document.querySelector('#mpg-users-table').classList.remove('d-none');

        },
        JSON.stringify(requestBody)
    );

};

/**
 * Adds an event listener on each database.
 * 
 * @returns {void}
 */
MPG.eventListeners.addDatabases = function() {

    document.querySelectorAll('.mpg-database-link').forEach(function(databaseLink) {

        databaseLink.addEventListener('click', function(_event) {
            
            MPG.databaseName = databaseLink.dataset.databaseName;
            MPG.helpers.completeNavLinks('#' + MPG.databaseName);

            document.querySelectorAll('.mpg-database-link').forEach(function(databaseLink) {
                databaseLink.classList.remove('active');
            });

            databaseLink.classList.add('active');

            document.querySelector('#mpg-please-select-a-db').classList.add('d-none');
            document.querySelector('#mpg-open-create-user-modal-button').classList.remove('d-none');

            MPG.reloadUsers(databaseLink.dataset.databaseName);

        });

    });

};

/**
 * Adds an event listener on "Create user" button.
 * 
 * @returns {void}
 */
MPG.eventListeners.addCreateUser = function() {

    document.querySelector('#mpg-create-user-button').addEventListener('click', function(_event) {

        if ( MPG.databaseName === '' ) {
            return window.alert('Please select a database.');
        }
        
        var userName = document.querySelector('#mpg-user-name').value;

        if ( userName.trim() === '' ) {
            return window.alert('Please enter an user name.');
        }

        var userPassword = document.querySelector('#mpg-user-password').value;

        if ( userPassword.trim() === '' ) {
            return window.alert('Please enter an user password.');
        }

        // TODO: Manage several roles per user.
        var userRole = document.querySelector('#mpg-user-role').value;
        var userRoleDatabase = document.querySelector('#mpg-user-role-database').value;

        var requestBody = {
            'databaseName': MPG.databaseName,
            'userName': userName,
            'userPassword': userPassword,
            'userRoles': [
                { 'role': userRole, 'db': userRoleDatabase }
            ]
        };

        MPG.helpers.doAjaxRequest(
            'POST',
            './createUser',
            function(response) {

                if ( JSON.parse(response) === true ) {

                    MPG.reloadUsers(MPG.databaseName);
                    document.querySelector('#mpg-create-user-modal').classList.remove('d-block');

                }

            },
            JSON.stringify(requestBody)
        );

    });

};

/**
 * Adds an event listener on "Drop user" buttons.
 * 
 * @returns {void}
 */
MPG.eventListeners.addDropUser = function() {

    var userDropButtons = document.querySelectorAll('.mpg-drop-user-button');

    userDropButtons.forEach(function(userDropButton) {

        userDropButton.addEventListener('click', function(_event) {

            var dropConfirmation = window.confirm(
                'Do you REALLY want to DROP user: ' + userDropButton.dataset.userName
            );
    
            if ( dropConfirmation === false ) {
                return;
            }

            var requestBody = {
                'databaseName': MPG.databaseName,
                'userName': userDropButton.dataset.userName
            };

            MPG.helpers.doAjaxRequest(
                'POST',
                './dropUser',
                function(response) {
    
                    if ( JSON.parse(response) === true ) {
                        MPG.reloadUsers(MPG.databaseName);
                    }
    
                },
                JSON.stringify(requestBody)
            );

        });

    });

};

// When document is ready:
window.addEventListener('DOMContentLoaded', function(_event) {

    MPG.eventListeners.addMenuToggle();
    MPG.eventListeners.addDatabases();
    MPG.eventListeners.addModalOpen();
    MPG.eventListeners.addCreateUser();
    MPG.eventListeners.addModalClose();

    MPG.helpers.navigateOnSamePage();

});
