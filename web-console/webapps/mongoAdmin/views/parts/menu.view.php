<nav class="navbar sticky-top">

    <a class="navbar-brand" href="./">
        <img class="navbar-brand-icon" src="./assets/images/mpg-icon.svg" width="24" height="24" />
        MongoDB PHP GUI
    </a>

    <div class="navbar-nav">
        <a class="nav-item nav-link<?php echo ('manageCollections' === $viewName) ? ' active' : ''; ?>" data-canonical-url="./manageCollections" href="./manageCollections">Manage collections</a>
        <a class="nav-item nav-link<?php echo ('importDocuments' === $viewName) ? ' active' : ''; ?>" data-canonical-url="./importDocuments" href="./importDocuments">Import documents</a>
        <a class="nav-item nav-link<?php echo ('visualizeDatabase' === $viewName) ? ' active' : ''; ?>" data-canonical-url="./visualizeDatabase" href="./visualizeDatabase">Visualize database</a>
        <a class="nav-item nav-link<?php echo ('queryDocuments' === $viewName) ? ' active' : ''; ?>" data-canonical-url="./queryDocuments" href="./queryDocuments">Query documents</a>
        <a class="nav-item nav-link<?php echo ('queryDatabase' === $viewName) ? ' active' : ''; ?>" data-canonical-url="./queryDatabase" href="./queryDatabase">Aggregation</a>
        <a class="nav-item nav-link<?php echo ('manageIndexes' === $viewName) ? ' active' : ''; ?>" data-canonical-url="./manageIndexes" href="./manageIndexes">Manage indexes</a>
        <a class="nav-item nav-link<?php echo ('manageUsers' === $viewName) ? ' active' : ''; ?>" data-canonical-url="./manageUsers" href="./manageUsers">Manage users</a>
        <a class="nav-item nav-link" href="./logout">Logout</a>
    </div>

    <button id="menu-toggle-button"><i class="fa fa-bars" aria-hidden="true"></i></button>

</nav>