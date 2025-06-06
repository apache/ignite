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

    <script src="./assets/js/vis-network.min.js"></script>

    <script src="./source/js/_base.js"></script>
    <script src="./source/js/visualizeDatabase.js"></script>
    
    <style type="text/css">
    #vis-network-container {
        float: left;
        width: 80%;	   
	}
	
    #info {
        float: right;
        width: 18%;
	    margin-top: 20px;
	    padding: 10px;
	    border: 1px solid lightgray;
	    display: block; /* Initially hidden */
	}
	
	#info table td {
		font-size: 14px;
	}
    
    </style>

</head>

<body>

    <?php require MPG\ABS_PATH . '/views/parts/menu.view.php'; ?>

    <div class="container-fluid">

        <div id="vis-network-container"></div>
        
        <div id="info" class="row"><?php echo $serverStatus; ?></div>
        
    </div>

</body>

</html>