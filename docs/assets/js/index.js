// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

import './code-tabs.js?1'
import {hideLeftNav} from './docs-menu.js'
//import {hideTopNav} from './top-navigation.js'
import './page-nav.js'




document.addEventListener('topNavigationShow', hideLeftNav)
document.addEventListener('leftNavigationShow', hideTopNav)




// enables search with Swiftype widget
jQuery(document).ready(function(){

    var customRenderFunction = function(document_type, item) {
        var out = '<a href="' + Swiftype.htmlEscape(item['url']) + '" class="st-search-result-link">' + item.highlight['title'] + '</a>';
        return out.concat('<p class="url">' + String(item['url']).replace("https://www.", '') + '</p><p class="body">' + item.highlight['body'] + '</p>');
    }
    
/*    jQuery("#search-input").swiftype({
        fetchFields: { 'page': ['url'] },
        renderFunction: customRenderFunction,
        highlightFields: {
            'page': {
                'title': {'size': 60, 'fallback': true },
                'body': { 'size': 100, 'fallback':true }
            }
        },
        engineKey: '_t6sDkq6YsFC_12W6UH2'
    });
    */
    
});
