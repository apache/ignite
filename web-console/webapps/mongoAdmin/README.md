# Free MongoDB GUI powered by PHP

Visually administrate your MongoDB database. Create, read, update & delete your documents.<br>
Query your MongoDB database with a [relax JSON syntax](#relaxed-json), regular expressions & SQL statements.<br>
Autocompletion is available for collection fields, MongoDB & SQL keywords via [key shortcuts](#key-shortcuts).<br>
Export documents to JSON. Import documents from JSON. Manage indexes. Manage users, etc.

## Screenshots

![MongoDB PHP GUI - Visualize Database](https://raw.githubusercontent.com/SamuelTallet/MongoDB-PHP-GUI/master/docs/screenshots/visualize-database.png)

![MongoDB PHP GUI - Query Documents](https://raw.githubusercontent.com/SamuelTallet/MongoDB-PHP-GUI/master/docs/screenshots/query-documents.png)

## Installation

### Docker (PHP built-in server)
1. In a case of an upgrade, run `docker pull samueltallet/mongodb-php-gui`<br>
2. Always run `docker run --add-host localhost:172.17.0.1 --publish 5000:5000 --rm samueltallet/mongodb-php-gui`<br>
3. Open your browser at this address: http://127.0.0.1:5000/ to access GUI.

### Apache HTTP server
1. Clone current repository in a folder served by Apache.
2. Be sure to have PHP >= 7.3 with [MongoDB extension](https://www.php.net/manual/en/mongodb.installation.php) enabled.
3. Check that `rewrite_module` module is enabled in your Apache configuration.
4. Be sure to have `AllowOverride All` in your Apache (virtual host) configuration.
5. Run `composer install` at project's root directory to install all PHP dependencies.
6. Optionnaly, if you want to query DB with SQL, you must have [Java JDK](https://jdk.java.net/) installed.
7. Open your browser at Apache server URL to access GUI.

## Usage

### Query Syntax

#### Relaxed JSON

MongoDB PHP GUI supports a relaxed JSON syntax. In practice, this query:

```js
city: New York
```

Will produce same result that:

```js
{ "city": "New York" }
```

#### Regular Expressions

Imagine you want to find all the US cities starting with "San An". This query:

```js
city: /^San An/
```

Will output:
- San Antonio (FL)
- San Angelo (TX)
- ...

#### SQL Statements

If Java JDK is installed, you can query MongoDB with SQL statements such as:

```sql
SELECT * FROM Cities WHERE state = "CA"
```

### Key Shortcuts

<kbd>Ctrl + Space</kbd> Autocomplete the query<br>
<kbd>Ctrl + *</kbd> Count doc(s) matching the query<br>
<kbd>Ctrl + Enter</kbd> Find doc(s) matching the query

## Credits

This GUI uses [Limber](https://github.com/nimbly/Limber), [Capsule](https://github.com/nimbly/Capsule), [Font Awesome](https://fontawesome.com/), [Bootstrap](https://getbootstrap.com/), [CodeMirror](https://github.com/codemirror/codemirror), [jsonic](https://github.com/jsonicjs/jsonic), [JsonView](https://github.com/pgrabovets/json-view), [MongoDB PHP library](https://github.com/mongodb/mongo-php-library), [vis.js](https://github.com/visjs) and [SQL to MongoDB Query Converter](https://github.com/vincentrussell/sql-to-mongo-db-query-converter). Leaf icon was made by [Freepik](https://www.freepik.com) from [Flaticon](https://www.flaticon.com).

## Funding

If you find this GUI useful, [donate](https://www.paypal.me/SamuelTallet) at least one dollar to support its development. Thank you to all! ❤️

## Copyright

© 2022 Samuel Tallet
