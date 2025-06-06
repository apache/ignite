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
    <link rel="stylesheet" href="./source/css/inner.css">

    <script src="./source/js/_base.js"></script>
    <script src="./source/js/importDocuments.js"></script>

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

                    <div class="col-md-12">
                        
                        <div id="mpg-import-notice" class="alert alert-info" role="alert">Imported docs are appended. Max file size: <?php echo $maxFileSize; ?></div>
                        
                        <?php
                        if ( !empty($successMessage) ) :
                        ?>
                            <h2>Result</h2>
                            <div class="alert alert-success" role="alert">Success: <?php echo $successMessage; ?></div>
                        <?php
                        endif;
                        ?>

                        <?php
                        if ( !empty($errorMessage) ) :
                        ?>
                            <h2>Result</h2>
                            <div class="alert alert-danger" role="alert">Error: <?php echo $errorMessage; ?></div>
                        <?php
                        endif;
                        ?>

                        <form id="mpg-import-form" method="POST" enctype="multipart/form-data">

                            <input id="mpg-import-file" type="file" accept=".json,.jsonl,.csv,.tsv" name="import" class="form-control-file d-inline align-middle">
                            <div class="form-group-auto-width">
								<div>
	                                Skip <input name="mpg-skip-input" type="number" class="form-control" value="0" min="0" />
	                            </div>
	                            <div>
	                                Limit <input name="mpg-limit-input" type="number" class="form-control" value="-1" min="1" />
	                            </div>
	                            <div>
	                                ID Field <input name="mpg-id-field-input" type="text" class="form-control" value="_id" min="1" />
	                            </div>
                            </div>

                            <input type="hidden" name="database_name" value="">
                            <input type="hidden" name="collection_name" value="">

                            <button id="mpg-import-button" type="submit" class="btn btn-primary d-inline">
                                Import
                            </button>

                        </form>

                    </div>

                </div>
                
            </div>
            
        </div>
        
    </div>

</body>

</html>