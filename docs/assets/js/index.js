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
