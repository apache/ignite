<!DOCTYPE html>
<html lang="en">
<head>

    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">

    <title>MongoDB PHP GUI v<?php echo MPG\VERSION; ?></title>

    <link rel="icon" href="./assets/images/mpg-icon.svg">
    <link rel="mask-icon" href="./assets/images/mpg-safari-icon.svg" color="#6eb825">
    <link rel="apple-touch-icon" href="./assets/images/mpg-ios-icon.png">

    <link rel="stylesheet" href="./assets/css/ubuntu-font.css">
    <link rel="stylesheet" href="./assets/css/fontawesome-custom.css">
    <link rel="stylesheet" href="./assets/css/bootstrap.min.css">
    <link rel="stylesheet" href="./assets/css/codemirror.min.css">
    <link rel="stylesheet" href="./assets/css/codemirror-show-hint.min.css">
    <link rel="stylesheet" href="./assets/css/jsonview.bundle.css">
    <link rel="stylesheet" href="./source/css/inner.css">

    <script src="./assets/js/codemirror.min.js"></script>
    <script src="./assets/js/codemirror-js-mode.js"></script>
    <script src="./assets/js/codemirror-sql-mode.js"></script>
    <script src="./assets/js/codemirror-show-hint.min.js"></script>
    <script src="./assets/js/codemirror-mpg-hint.js"></script>
    <script src="./assets/js/jsonic.min.js"></script>

    <script src="./source/js/_base.js"></script>
    <script src="./source/js/jsonViewModified.js"></script>
    <script src="./source/js/queryDocuments.js"></script>

</head>

<body>

    <?php require MPG\ABS_PATH . '/views/parts/menu.view.php'; ?>

    <div class="container-fluid">

        <div class="row">

            <div class="col-md-3">

                <div class="row">

                    <div class="col-md-12">

                        <?php require MPG\ABS_PATH . '/views/parts/databases.view.php'; ?>

                    </div>
                    
                </div>
                
                <div class="row">

                    <div class="col-md-12">

                        <h2>Collections</h2>

                        <ul id="mpg-collections-list">
                            <li><i>Please select a database.</i></li>
                        </ul>

                    </div>

                </div>
                
            </div>
            
            <div class="col-md-9">

                <div class="row">

                    <div class="col-md-8">

                        <h2 class="float-left">Document</h2>

                        <h2 class="float-right">Filter</h2>

                        <textarea id="mpg-filter-or-doc-textarea" rows="5" ></textarea>

                        <button id="mpg-insert-one-button" class="btn btn-primary float-left">
                            Insert one
                        </button>

                        <button id="mpg-find-button" class="btn btn-success float-right" title="Ctrl + Enter">
                            Find
                        </button>

                        <button id="mpg-delete-one-button" class="btn btn-danger float-right">
                            Delete one
                        </button>

                        <button id="mpg-count-button" class="btn btn-info float-right" title="Ctrl + *">
                            Count
                        </button>

                    </div>
                    
                    <div class="col-md-4">

                        <h2>Options</h2>
                        
                        <div class="form-group">
							<div>
                                Skip <input id="mpg-skip-input" type="number" class="form-control" value="0" min="0">
                            </div>
                            <div>
                                Limit <input id="mpg-limit-input" type="number" class="form-control" value="10" min="1">
                            </div>
                            
                            <div>
                                Sort
                                <select id="mpg-sort-select" class="form-control">
                                    <option value="">Please select a database and a collection.</option>
                                </select>
                            </div>
                            
                            <div>
                                Order
                                <select id="mpg-order-select" class="form-control">
                                    <option value="1">ASC</option>
                                    <option value="-1">DESC</option>
                                </select>
                            </div>

                        </div>

                    </div>
                    
                </div>
                
                <div class="row">

                    <div id="mpg-output-column" class="col-md-12">

                        <h2 class="d-inline-block">Output</h2>

                        <button id="mpg-export-button" class="btn btn-secondary">Export</button>
                        
                        <div>
                            <code id="mpg-output-code"></code>
                        </div>

                    </div>
                    
                </div>
                
            </div>
            
        </div>
        
    </div>

</body>

</html>