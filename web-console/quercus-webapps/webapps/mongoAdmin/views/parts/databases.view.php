<h2>Databases</h2>

<ul id="mpg-databases-list">
    <?php foreach ($databaseNames as $databaseName) : ?>
    <li>
        <i class="fa fa-database" aria-hidden="true"></i>
        <a class="mpg-database-link" data-database-name="<?php echo $databaseName; ?>" href="#<?php echo $databaseName; ?>">
            <?php echo $databaseName; ?>
        </a>
    </li>
    <?php endforeach; ?>
</ul>