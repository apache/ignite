/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
