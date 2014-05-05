/*
 * @js.file.header
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 *
 * Version: @js.file.version
 */

/*
 * Sets the display style of a given element to either "none" or "block",
 * hiding or displaying the element on the page.
 */
function toggleDisplay(id) {
    var e = document.getElementById(id);

    if (e.style.display != 'none') {
        e.style.display = 'none';
        e.style.visibility = 'hidden';
    }
    else {
        e.style.display = 'block';
        e.style.visibility = 'visible';
    }
}

/*
 * Produces forum search form.
 */
function forumSearchForm(root) {
    document.write(
        "<form  method='POST' action='http://www.gridgainsystems.com/jiveforums/search.jspa' target='forum' style='margin: 1px; padding: 1px'>" +
            "" +
            "<input class='search_text' type='text' style='color: #ccc' onClick='this.value=\"\"; this.style.color=\"#333\"' name='q' value=' find...' size='20' maxlength='100'>" +
            "&nbsp;" +
            "<input title='Search Forum' class='search_button' name='button' type='submit' value='f o r u m'>" +
        "</form>"
    );
}

/*
 * Produces Wiki search form.
 */
function wikiSearchForm(root) {
    document.write(
        "<form method='POST' action='http://www.gridgainsystems.com:8080/wiki/dosearchsite.action' target='wiki' style='margin: 1px; padding: 1px' name='search_form'>" +
            "" +
            "<input type='hidden' name='quickSearch' value='true'>" +
            "<input type='hidden' name='searchQuery.spaceKey' value='conf_global'>" +
            "<input class='search_text' type='text' style='color: #ccc' onClick='this.value=\"\"; this.style.color=\"#333\"' accessKey='s' value=' find...' name='searchQuery.queryString' size='20'>" +
            "&nbsp;" +
            "<input title='Search Wiki' class='search_button' name='button' type='submit' value='w i k i'>" +
        "</form>"
    );
}
