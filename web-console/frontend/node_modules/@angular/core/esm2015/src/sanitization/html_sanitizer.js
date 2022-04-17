/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { isDevMode } from '../util/is_dev_mode';
import { InertBodyHelper } from './inert_body';
import { _sanitizeUrl, sanitizeSrcset } from './url_sanitizer';
/**
 * @param {?} tags
 * @return {?}
 */
function tagSet(tags) {
    /** @type {?} */
    const res = {};
    for (const t of tags.split(','))
        res[t] = true;
    return res;
}
/**
 * @param {...?} sets
 * @return {?}
 */
function merge(...sets) {
    /** @type {?} */
    const res = {};
    for (const s of sets) {
        for (const v in s) {
            if (s.hasOwnProperty(v))
                res[v] = true;
        }
    }
    return res;
}
// Good source of info about elements and attributes
// http://dev.w3.org/html5/spec/Overview.html#semantics
// http://simon.html5.org/html-elements
// Safe Void Elements - HTML5
// http://dev.w3.org/html5/spec/Overview.html#void-elements
/** @type {?} */
const VOID_ELEMENTS = tagSet('area,br,col,hr,img,wbr');
// Elements that you can, intentionally, leave open (and which close themselves)
// http://dev.w3.org/html5/spec/Overview.html#optional-tags
/** @type {?} */
const OPTIONAL_END_TAG_BLOCK_ELEMENTS = tagSet('colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr');
/** @type {?} */
const OPTIONAL_END_TAG_INLINE_ELEMENTS = tagSet('rp,rt');
/** @type {?} */
const OPTIONAL_END_TAG_ELEMENTS = merge(OPTIONAL_END_TAG_INLINE_ELEMENTS, OPTIONAL_END_TAG_BLOCK_ELEMENTS);
// Safe Block Elements - HTML5
/** @type {?} */
const BLOCK_ELEMENTS = merge(OPTIONAL_END_TAG_BLOCK_ELEMENTS, tagSet('address,article,' +
    'aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,' +
    'h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul'));
// Inline Elements - HTML5
/** @type {?} */
const INLINE_ELEMENTS = merge(OPTIONAL_END_TAG_INLINE_ELEMENTS, tagSet('a,abbr,acronym,audio,b,' +
    'bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,' +
    'samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video'));
/** @type {?} */
export const VALID_ELEMENTS = merge(VOID_ELEMENTS, BLOCK_ELEMENTS, INLINE_ELEMENTS, OPTIONAL_END_TAG_ELEMENTS);
// Attributes that have href and hence need to be sanitized
/** @type {?} */
export const URI_ATTRS = tagSet('background,cite,href,itemtype,longdesc,poster,src,xlink:href');
// Attributes that have special href set hence need to be sanitized
/** @type {?} */
export const SRCSET_ATTRS = tagSet('srcset');
/** @type {?} */
const HTML_ATTRS = tagSet('abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,' +
    'compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,' +
    'ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,' +
    'scope,scrolling,shape,size,sizes,span,srclang,start,summary,tabindex,target,title,translate,type,usemap,' +
    'valign,value,vspace,width');
// Accessibility attributes as per WAI-ARIA 1.1 (W3C Working Draft 14 December 2018)
/** @type {?} */
const ARIA_ATTRS = tagSet('aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,' +
    'aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,' +
    'aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,' +
    'aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,' +
    'aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,' +
    'aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,' +
    'aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext');
// NB: This currently consciously doesn't support SVG. SVG sanitization has had several security
// issues in the past, so it seems safer to leave it out if possible. If support for binding SVG via
// innerHTML is required, SVG attributes should be added here.
// NB: Sanitization does not allow <form> elements or other active elements (<button> etc). Those
// can be sanitized, but they increase security surface area without a legitimate use case, so they
// are left out here.
/** @type {?} */
export const VALID_ATTRS = merge(URI_ATTRS, SRCSET_ATTRS, HTML_ATTRS, ARIA_ATTRS);
// Elements whose content should not be traversed/preserved, if the elements themselves are invalid.
//
// Typically, `<invalid>Some content</invalid>` would traverse (and in this case preserve)
// `Some content`, but strip `invalid-element` opening/closing tags. For some elements, though, we
// don't want to preserve the content, if the elements themselves are going to be removed.
/** @type {?} */
const SKIP_TRAVERSING_CONTENT_IF_INVALID_ELEMENTS = tagSet('script,style,template');
/**
 * SanitizingHtmlSerializer serializes a DOM fragment, stripping out any unsafe elements and unsafe
 * attributes.
 */
class SanitizingHtmlSerializer {
    constructor() {
        // Explicitly track if something was stripped, to avoid accidentally warning of sanitization just
        // because characters were re-encoded.
        this.sanitizedSomething = false;
        this.buf = [];
    }
    /**
     * @param {?} el
     * @return {?}
     */
    sanitizeChildren(el) {
        // This cannot use a TreeWalker, as it has to run on Angular's various DOM adapters.
        // However this code never accesses properties off of `document` before deleting its contents
        // again, so it shouldn't be vulnerable to DOM clobbering.
        /** @type {?} */
        let current = (/** @type {?} */ (el.firstChild));
        /** @type {?} */
        let traverseContent = true;
        while (current) {
            if (current.nodeType === Node.ELEMENT_NODE) {
                traverseContent = this.startElement((/** @type {?} */ (current)));
            }
            else if (current.nodeType === Node.TEXT_NODE) {
                this.chars((/** @type {?} */ (current.nodeValue)));
            }
            else {
                // Strip non-element, non-text nodes.
                this.sanitizedSomething = true;
            }
            if (traverseContent && current.firstChild) {
                current = (/** @type {?} */ (current.firstChild));
                continue;
            }
            while (current) {
                // Leaving the element. Walk up and to the right, closing tags as we go.
                if (current.nodeType === Node.ELEMENT_NODE) {
                    this.endElement((/** @type {?} */ (current)));
                }
                /** @type {?} */
                let next = this.checkClobberedElement(current, (/** @type {?} */ (current.nextSibling)));
                if (next) {
                    current = next;
                    break;
                }
                current = this.checkClobberedElement(current, (/** @type {?} */ (current.parentNode)));
            }
        }
        return this.buf.join('');
    }
    /**
     * Sanitizes an opening element tag (if valid) and returns whether the element's contents should
     * be traversed. Element content must always be traversed (even if the element itself is not
     * valid/safe), unless the element is one of `SKIP_TRAVERSING_CONTENT_IF_INVALID_ELEMENTS`.
     *
     * @private
     * @param {?} element The element to sanitize.
     * @return {?} True if the element's contents should be traversed.
     */
    startElement(element) {
        /** @type {?} */
        const tagName = element.nodeName.toLowerCase();
        if (!VALID_ELEMENTS.hasOwnProperty(tagName)) {
            this.sanitizedSomething = true;
            return !SKIP_TRAVERSING_CONTENT_IF_INVALID_ELEMENTS.hasOwnProperty(tagName);
        }
        this.buf.push('<');
        this.buf.push(tagName);
        /** @type {?} */
        const elAttrs = element.attributes;
        for (let i = 0; i < elAttrs.length; i++) {
            /** @type {?} */
            const elAttr = elAttrs.item(i);
            /** @type {?} */
            const attrName = (/** @type {?} */ (elAttr)).name;
            /** @type {?} */
            const lower = attrName.toLowerCase();
            if (!VALID_ATTRS.hasOwnProperty(lower)) {
                this.sanitizedSomething = true;
                continue;
            }
            /** @type {?} */
            let value = (/** @type {?} */ (elAttr)).value;
            // TODO(martinprobst): Special case image URIs for data:image/...
            if (URI_ATTRS[lower])
                value = _sanitizeUrl(value);
            if (SRCSET_ATTRS[lower])
                value = sanitizeSrcset(value);
            this.buf.push(' ', attrName, '="', encodeEntities(value), '"');
        }
        this.buf.push('>');
        return true;
    }
    /**
     * @private
     * @param {?} current
     * @return {?}
     */
    endElement(current) {
        /** @type {?} */
        const tagName = current.nodeName.toLowerCase();
        if (VALID_ELEMENTS.hasOwnProperty(tagName) && !VOID_ELEMENTS.hasOwnProperty(tagName)) {
            this.buf.push('</');
            this.buf.push(tagName);
            this.buf.push('>');
        }
    }
    /**
     * @private
     * @param {?} chars
     * @return {?}
     */
    chars(chars) { this.buf.push(encodeEntities(chars)); }
    /**
     * @param {?} node
     * @param {?} nextNode
     * @return {?}
     */
    checkClobberedElement(node, nextNode) {
        if (nextNode &&
            (node.compareDocumentPosition(nextNode) &
                Node.DOCUMENT_POSITION_CONTAINED_BY) === Node.DOCUMENT_POSITION_CONTAINED_BY) {
            throw new Error(`Failed to sanitize html because the element is clobbered: ${((/** @type {?} */ (node))).outerHTML}`);
        }
        return nextNode;
    }
}
if (false) {
    /** @type {?} */
    SanitizingHtmlSerializer.prototype.sanitizedSomething;
    /**
     * @type {?}
     * @private
     */
    SanitizingHtmlSerializer.prototype.buf;
}
// Regular Expressions for parsing tags and attributes
/** @type {?} */
const SURROGATE_PAIR_REGEXP = /[\uD800-\uDBFF][\uDC00-\uDFFF]/g;
// ! to ~ is the ASCII range.
/** @type {?} */
const NON_ALPHANUMERIC_REGEXP = /([^\#-~ |!])/g;
/**
 * Escapes all potentially dangerous characters, so that the
 * resulting string can be safely inserted into attribute or
 * element text.
 * @param {?} value
 * @return {?}
 */
function encodeEntities(value) {
    return value.replace(/&/g, '&amp;')
        .replace(SURROGATE_PAIR_REGEXP, (/**
     * @param {?} match
     * @return {?}
     */
    function (match) {
        /** @type {?} */
        const hi = match.charCodeAt(0);
        /** @type {?} */
        const low = match.charCodeAt(1);
        return '&#' + (((hi - 0xD800) * 0x400) + (low - 0xDC00) + 0x10000) + ';';
    }))
        .replace(NON_ALPHANUMERIC_REGEXP, (/**
     * @param {?} match
     * @return {?}
     */
    function (match) { return '&#' + match.charCodeAt(0) + ';'; }))
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}
/** @type {?} */
let inertBodyHelper;
/**
 * Sanitizes the given unsafe, untrusted HTML fragment, and returns HTML text that is safe to add to
 * the DOM in a browser environment.
 * @param {?} defaultDoc
 * @param {?} unsafeHtmlInput
 * @return {?}
 */
export function _sanitizeHtml(defaultDoc, unsafeHtmlInput) {
    /** @type {?} */
    let inertBodyElement = null;
    try {
        inertBodyHelper = inertBodyHelper || new InertBodyHelper(defaultDoc);
        // Make sure unsafeHtml is actually a string (TypeScript types are not enforced at runtime).
        /** @type {?} */
        let unsafeHtml = unsafeHtmlInput ? String(unsafeHtmlInput) : '';
        inertBodyElement = inertBodyHelper.getInertBodyElement(unsafeHtml);
        // mXSS protection. Repeatedly parse the document to make sure it stabilizes, so that a browser
        // trying to auto-correct incorrect HTML cannot cause formerly inert HTML to become dangerous.
        /** @type {?} */
        let mXSSAttempts = 5;
        /** @type {?} */
        let parsedHtml = unsafeHtml;
        do {
            if (mXSSAttempts === 0) {
                throw new Error('Failed to sanitize html because the input is unstable');
            }
            mXSSAttempts--;
            unsafeHtml = parsedHtml;
            parsedHtml = (/** @type {?} */ (inertBodyElement)).innerHTML;
            inertBodyElement = inertBodyHelper.getInertBodyElement(unsafeHtml);
        } while (unsafeHtml !== parsedHtml);
        /** @type {?} */
        const sanitizer = new SanitizingHtmlSerializer();
        /** @type {?} */
        const safeHtml = sanitizer.sanitizeChildren((/** @type {?} */ (getTemplateContent((/** @type {?} */ (inertBodyElement))))) || inertBodyElement);
        if (isDevMode() && sanitizer.sanitizedSomething) {
            console.warn('WARNING: sanitizing HTML stripped some content, see http://g.co/ng/security#xss');
        }
        return safeHtml;
    }
    finally {
        // In case anything goes wrong, clear out inertElement to reset the entire DOM structure.
        if (inertBodyElement) {
            /** @type {?} */
            const parent = getTemplateContent(inertBodyElement) || inertBodyElement;
            while (parent.firstChild) {
                parent.removeChild(parent.firstChild);
            }
        }
    }
}
/**
 * @param {?} el
 * @return {?}
 */
export function getTemplateContent(el) {
    return 'content' in ((/** @type {?} */ (el))) && isTemplateElement(el) ?
        el.content :
        null;
}
/**
 * @param {?} el
 * @return {?}
 */
function isTemplateElement(el) {
    return el.nodeType === Node.ELEMENT_NODE && el.nodeName === 'TEMPLATE';
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaHRtbF9zYW5pdGl6ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9zYW5pdGl6YXRpb24vaHRtbF9zYW5pdGl6ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0scUJBQXFCLENBQUM7QUFDOUMsT0FBTyxFQUFDLGVBQWUsRUFBQyxNQUFNLGNBQWMsQ0FBQztBQUM3QyxPQUFPLEVBQUMsWUFBWSxFQUFFLGNBQWMsRUFBQyxNQUFNLGlCQUFpQixDQUFDOzs7OztBQUU3RCxTQUFTLE1BQU0sQ0FBQyxJQUFZOztVQUNwQixHQUFHLEdBQTJCLEVBQUU7SUFDdEMsS0FBSyxNQUFNLENBQUMsSUFBSSxJQUFJLENBQUMsS0FBSyxDQUFDLEdBQUcsQ0FBQztRQUFFLEdBQUcsQ0FBQyxDQUFDLENBQUMsR0FBRyxJQUFJLENBQUM7SUFDL0MsT0FBTyxHQUFHLENBQUM7QUFDYixDQUFDOzs7OztBQUVELFNBQVMsS0FBSyxDQUFDLEdBQUcsSUFBOEI7O1VBQ3hDLEdBQUcsR0FBMkIsRUFBRTtJQUN0QyxLQUFLLE1BQU0sQ0FBQyxJQUFJLElBQUksRUFBRTtRQUNwQixLQUFLLE1BQU0sQ0FBQyxJQUFJLENBQUMsRUFBRTtZQUNqQixJQUFJLENBQUMsQ0FBQyxjQUFjLENBQUMsQ0FBQyxDQUFDO2dCQUFFLEdBQUcsQ0FBQyxDQUFDLENBQUMsR0FBRyxJQUFJLENBQUM7U0FDeEM7S0FDRjtJQUNELE9BQU8sR0FBRyxDQUFDO0FBQ2IsQ0FBQzs7Ozs7OztNQVFLLGFBQWEsR0FBRyxNQUFNLENBQUMsd0JBQXdCLENBQUM7Ozs7TUFJaEQsK0JBQStCLEdBQUcsTUFBTSxDQUFDLGdEQUFnRCxDQUFDOztNQUMxRixnQ0FBZ0MsR0FBRyxNQUFNLENBQUMsT0FBTyxDQUFDOztNQUNsRCx5QkFBeUIsR0FDM0IsS0FBSyxDQUFDLGdD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LEVBQUUsQ0FBQyxDQUFDLENBQUM7UUFDeEYsRUFBRSxDQUFDLE9BQU8sQ0FBQyxDQUFDO1FBQ1osSUFBSSxDQUFDO0FBQ1gsQ0FBQzs7Ozs7QUFDRCxTQUFTLGlCQUFpQixDQUFDLEVBQVE7SUFDakMsT0FBTyxFQUFFLENBQUMsUUFBUSxLQUFLLElBQUksQ0FBQyxZQUFZLElBQUksRUFBRSxDQUFDLFFBQVEsS0FBSyxVQUFVLENBQUM7QUFDekUsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtpc0Rldk1vZGV9IGZyb20gJy4uL3V0aWwvaXNfZGV2X21vZGUnO1xuaW1wb3J0IHtJbmVydEJvZHlIZWxwZXJ9IGZyb20gJy4vaW5lcnRfYm9keSc7XG5pbXBvcnQge19zYW5pdGl6ZVVybCwgc2FuaXRpemVTcmNzZXR9IGZyb20gJy4vdXJsX3Nhbml0aXplcic7XG5cbmZ1bmN0aW9uIHRhZ1NldCh0YWdzOiBzdHJpbmcpOiB7W2s6IHN0cmluZ106IGJvb2xlYW59IHtcbiAgY29uc3QgcmVzOiB7W2s6IHN0cmluZ106IGJvb2xlYW59ID0ge307XG4gIGZvciAoY29uc3QgdCBvZiB0YWdzLnNwbGl0KCcsJykpIHJlc1t0XSA9IHRydWU7XG4gIHJldHVybiByZXM7XG59XG5cbmZ1bmN0aW9uIG1lcmdlKC4uLnNldHM6IHtbazogc3RyaW5nXTogYm9vbGVhbn1bXSk6IHtbazogc3RyaW5nXTogYm9vbGVhbn0ge1xuICBjb25zdCByZXM6IHtbazogc3RyaW5nXTogYm9vbGVhbn0gPSB7fTtcbiAgZm9yIChjb25zdCBzIG9mIHNldHMpIHtcbiAgICBmb3IgKGNvbnN0IHYgaW4gcykge1xuICAgICAgaWYgKHMuaGFzT3duUHJvcGVydHkodikpIHJlc1t2XSA9IHRydWU7XG4gICAgfVxuICB9XG4gIHJldHVybiByZXM7XG59XG5cbi8vIEdvb2Qgc291cmNlIG9mIGluZm8gYWJvdXQgZWxlbWVudHMgYW5kIGF0dHJpYnV0ZXNcbi8vIGh0dHA6Ly9kZXYudzMub3JnL2h0bWw1L3NwZWMvT3ZlcnZpZXcuaHRtbCNzZW1hbnRpY3Ncbi8vIGh0dHA6Ly9zaW1vbi5odG1sNS5vcmcvaHRtbC1lbGVtZW50c1xuXG4vLyBTYWZlIFZvaWQgRWxlbWVudHMgLSBIVE1MNVxuLy8gaHR0cDovL2Rldi53My5vcmcvaHRtbDUvc3BlYy9PdmVydmlldy5odG1sI3ZvaWQtZWxlbWVudHNcbmNvbnN0IFZPSURfRUxFTUVOVFMgPSB0YWdTZXQoJ2FyZWEsYnIsY29sLGhyLGltZyx3YnInKTtcblxuLy8gRWxlbWVudHMgdGhhdCB5b3UgY2FuLCBpbnRlbnRpb25hbGx5LCBsZWF2ZSBvcGVuIChhbmQgd2hpY2ggY2xvc2UgdGhlbXNlbHZlcylcbi8vIGh0dHA6Ly9kZXYudzMub3JnL2h0bWw1L3NwZWMvT3ZlcnZpZXcuaHRtbCNvcHRpb25hbC10YWdzXG5jb25zdCBPUFRJT05BTF9FTkRfVEFHX0JMT0NLX0VMRU1FTlRTID0gdGFnU2V0KCdjb2xncm91cCxkZCxkdCxsaSxwLHRib2R5LHRkLHRmb290LHRoLHRoZWFkLHRyJyk7XG5jb25zdCBPUFRJT05BTF9FTkRfVEFHX0lOTElORV9FTEVNRU5UUyA9IHRhZ1NldCgncnAscnQnKTtcbmNvbnN0IE9QVElPTkFMX0VORF9UQUdfRUxFTUVOVFMgPVxuICAgIG1lcmdlKE9QVElPTkFMX0VORF9UQUdfSU5MSU5FX0VMRU1FTlRTLCBPUFRJT05BTF9FTkRfVEFHX0JMT0NLX0VMRU1FTlRTKTtcblxuLy8gU2FmZSBCbG9jayBFbGVtZW50cyAtIEhUTUw1XG5jb25zdCBCTE9DS19FTEVNRU5UUyA9IG1lcmdlKFxuICAgIE9QVElPTkFMX0VORF9UQUdfQkxPQ0tfRUxFTUVOVFMsXG4gICAgdGFnU2V0KFxuICAgICAgICAnYWRkcmVzcyxhcnRpY2xlLCcgK1xuICAgICAgICAnYXNpZGUsYmxvY2txdW90ZSxjYXB0aW9uLGNlbnRlcixkZWwsZGV0YWlscyxkaWFsb2csZGlyLGRpdixkbCxmaWd1cmUsZmlnY2FwdGlvbixmb290ZXIsaDEsaDIsaDMsaDQsaDUsJyArXG4gICAgICAgICdoNixoZWFkZXIsaGdyb3VwLGhyLGlucyxtYWluLG1hcCxtZW51LG5hdixvbCxwcmUsc2VjdGlvbixzdW1tYXJ5LHRhYmxlLHVsJykpO1xuXG4vLyBJbmxpbmUgRWxlbWVudHMgLSBIVE1MNVxuY29uc3QgSU5MSU5FX0VMRU1FTlRTID0gbWVyZ2UoXG4gICAgT1BUSU9OQUxfRU5EX1RBR19JTkxJTkVfRUxFTUVOVFMsXG4gICAgdGFnU2V0KFxuICAgICAgICAnYSxhYmJyLGFjcm9ueW0sYXVkaW8sYiwnICtcbiAgICAgICAgJ2JkaSxiZG8sYmlnLGJyLGNpdGUsY29kZSxkZWwsZGZuLGVtLGZvbnQsaSxpbWcsaW5zLGtiZCxsYWJlbCxtYXAsbWFyayxwaWN0dXJlLHEscnVieSxycCxydCxzLCcgK1xuICAgICAgICAnc2FtcCxzbWFsbCxzb3VyY2Usc3BhbixzdHJpa2Usc3Ryb25nLHN1YixzdXAsdGltZSx0cmFjayx0dCx1LHZhcix2aWRlbycpKTtcblxuZXhwb3J0IGNvbnN0IFZBTElEX0VMRU1FTlRTID1cbiAgICBtZXJnZShWT0lEX0VMRU1FTlRTLCBCTE9DS19FTEVNRU5UUywgSU5MSU5FX0VMRU1FTlRTLCBPUFRJT05BTF9FTkRfVEFHX0VMRU1FTlRTKTtcblxuLy8gQXR0cmlidXRlcyB0aGF0IGhhdmUgaHJlZiBhbmQgaGVuY2UgbmVlZCB0byBiZSBzYW5pdGl6ZWRcbmV4cG9ydCBjb25zdCBVUklfQVRUUlMgPSB0YWdTZXQoJ2JhY2tncm91bmQsY2l0ZSxocmVmLGl0ZW10eXBlLGxvbmdkZXNjLHBvc3RlcixzcmMseGxpbms6aHJlZicpO1xuXG4vLyBBdHRyaWJ1dGVzIHRoYXQgaGF2ZSBzcGVjaWFsIGhyZWYgc2V0IGhlbmNlIG5lZWQgdG8gYmUgc2FuaXRpemVkXG5leHBvcnQgY29uc3QgU1JDU0VUX0FUVFJTID0gdGFnU2V0KCdzcmNzZXQnKTtcblxuY29uc3QgSFRNTF9BVFRSUyA9IHRhZ1NldChcbiAgICAnYWJicixhY2Nlc3NrZXksYWxpZ24sYWx0LGF1dG9wbGF5LGF4aXMsYmdjb2xvcixib3JkZXIsY2VsbHBhZGRpbmcsY2VsbHNwYWNpbmcsY2xhc3MsY2xlYXIsY29sb3IsY29scyxjb2xzcGFuLCcgK1xuICAgICdjb21wYWN0LGNvbnRyb2xzLGNvb3JkcyxkYXRldGltZSxkZWZhdWx0LGRpcixkb3dubG9hZCxmYWNlLGhlYWRlcnMsaGVpZ2h0LGhpZGRlbixocmVmbGFuZyxoc3BhY2UsJyArXG4gICAgJ2lzbWFwLGl0ZW1zY29wZSxpdGVtcHJvcCxraW5kLGxhYmVsLGxhbmcsbGFuZ3VhZ2UsbG9vcCxtZWRpYSxtdXRlZCxub2hyZWYsbm93cmFwLG9wZW4scHJlbG9hZCxyZWwscmV2LHJvbGUscm93cyxyb3dzcGFuLHJ1bGVzLCcgK1xuICAgICdzY29wZSxzY3JvbGxpbmcsc2hhcGUsc2l6ZSxzaXplcyxzcGFuLHNyY2xhbmcsc3RhcnQsc3VtbWFyeSx0YWJpbmRleCx0YXJnZXQsdGl0bGUsdHJhbnNsYXRlLHR5cGUsdXNlbWFwLCcgK1xuICAgICd2YWxpZ24sdmFsdWUsdnNwYWNlLHdpZHRoJyk7XG5cbi8vIEFjY2Vzc2liaWxpdHkgYXR0cmlidXRlcyBhcyBwZXIgV0FJLUFSSUEgMS4xIChXM0MgV29ya2luZyBEcmFmdCAxNCBEZWNlbWJlciAyMDE4KVxuY29uc3QgQVJJQV9BVFRSUyA9IHRhZ1NldChcbiAgICAnYXJpYS1hY3RpdmVkZXNjZW5kYW50LGFyaWEtYXRvbWljLGFyaWEtYXV0b2NvbXBsZXRlLGFyaWEtYnVzeSxhcmlhLWNoZWNrZWQsYXJpYS1jb2xjb3VudCxhcmlhLWNvbGluZGV4LCcgK1xuICAgICdhcmlhLWNvbHNwYW4sYXJpYS1jb250cm9scyxhcmlhLWN1cnJlbnQsYXJpYS1kZXNjcmliZWRieSxhcmlhLWRldGFpbHMsYXJpYS1kaXNhYmxlZCxhcmlhLWRyb3BlZmZlY3QsJyArXG4gICAgJ2FyaWEtZXJyb3JtZXNzYWdlLGFyaWEtZXhwYW5kZWQsYXJpYS1mbG93dG8sYXJpYS1ncmFiYmVkLGFyaWEtaGFzcG9wdXAsYXJpYS1oaWRkZW4sYXJpYS1pbnZhbGlkLCcgK1xuICAgICdhcmlhLWtleXNob3J0Y3V0cyxhcmlhLWxhYmVsLGFyaWEtbGFiZWxsZWRieSxhcmlhLWxldmVsLGFyaWEtbGl2ZSxhcmlhLW1vZGFsLGFyaWEtbXVsdGlsaW5lLCcgK1xuICAgICdhcmlhLW11bHRpc2VsZWN0YWJsZSxhcmlhLW9yaWVudGF0aW9uLGFyaWEtb3ducyxhcmlhLXBsYWNlaG9sZGVyLGFyaWEtcG9zaW5zZXQsYXJpYS1wcmVzc2VkLGFyaWEtcmVhZG9ubHksJyArXG4gICAgJ2FyaWEtcmVsZXZhbnQsYXJpYS1yZXF1aXJlZCxhcmlhLXJvbGVkZXNjcmlwdGlvbixhcmlhLXJvd2NvdW50LGFyaWEtcm93aW5kZXgsYXJpYS1yb3dzcGFuLGFyaWEtc2VsZWN0ZWQsJyArXG4gICAgJ2FyaWEtc2V0c2l6ZSxhcmlhLXNvcnQsYXJpYS12YWx1ZW1heCxhcmlhLXZhbHVlbWluLGFyaWEtdmFsdWVub3csYXJpYS12YWx1ZXRleHQnKTtcblxuLy8gTkI6IFRoaXMgY3VycmVudGx5IGNvbnNjaW91c2x5IGRvZXNuJ3Qgc3VwcG9ydCBTVkcuIFNWRyBzYW5pdGl6YXRpb24gaGFzIGhhZCBzZXZlcmFsIHNlY3VyaXR5XG4vLyBpc3N1ZXMgaW4gdGhlIHBhc3QsIHNvIGl0IHNlZW1zIHNhZmVyIHRvIGxlYXZlIGl0IG91dCBpZiBwb3NzaWJsZS4gSWYgc3VwcG9ydCBmb3IgYmluZGluZyBTVkcgdmlhXG4vLyBpbm5lckhUTUwgaXMgcmVxdWlyZWQsIFNWRyBhdHRyaWJ1dGVzIHNob3VsZCBiZSBhZGRlZCBoZXJlLlxuXG4vLyBOQjogU2FuaXRpemF0aW9uIGRvZXMgbm90IGFsbG93IDxmb3JtPiBlbGVtZW50cyBvciBvdGhlciBhY3RpdmUgZWxlbWVudHMgKDxidXR0b24+IGV0YykuIFRob3NlXG4vLyBjYW4gYmUgc2FuaXRpemVkLCBidXQgdGhleSBpbmNyZWFzZSBzZWN1cml0eSBzdXJmYWNlIGFyZWEgd2l0aG91dCBhIGxlZ2l0aW1hdGUgdXNlIGNhc2UsIHNvIHRoZXlcbi8vIGFyZSBsZWZ0IG91dCBoZXJlLlxuXG5leHBvcnQgY29uc3QgVkFMSURfQVRUUlMgPSBtZXJnZShVUklfQVRUUlMsIFNSQ1NFVF9BVFRSUywgSFRNTF9BVFRSUywgQVJJQV9BVFRSUyk7XG5cbi8vIEVsZW1lbnRzIHdob3NlIGNvbnRlbnQgc2hvdWxkIG5vdCBiZSB0cmF2ZXJzZWQvcHJlc2VydmVkLCBpZiB0aGUgZWxlbWVudHMgdGhlbXNlbHZlcyBhcmUgaW52YWxpZC5cbi8vXG4vLyBUeXBpY2FsbHksIGA8aW52YWxpZD5Tb21lIGNvbnRlbnQ8L2ludmFsaWQ+YCB3b3VsZCB0cmF2ZXJzZSAoYW5kIGluIHRoaXMgY2FzZSBwcmVzZXJ2ZSlcbi8vIGBTb21lIGNvbnRlbnRgLCBidXQgc3RyaXAgYGludmFsaWQtZWxlbWVudGAgb3BlbmluZy9jbG9zaW5nIHRhZ3MuIEZvciBzb21lIGVsZW1lbnRzLCB0aG91Z2gsIHdlXG4vLyBkb24ndCB3YW50IHRvIHByZXNlcnZlIHRoZSBjb250ZW50LCBpZiB0aGUgZWxlbWVudHMgdGhlbXNlbHZlcyBhcmUgZ29pbmcgdG8gYmUgcmVtb3ZlZC5cbmNvbnN0IFNLSVBfVFJBVkVSU0lOR19DT05URU5UX0lGX0lOVkFMSURfRUxFTUVOVFMgPSB0YWdTZXQoJ3NjcmlwdCxzdHlsZSx0ZW1wbGF0ZScpO1xuXG4vKipcbiAqIFNhbml0aXppbmdIdG1sU2VyaWFsaXplciBzZXJpYWxpemVzIGEgRE9NIGZyYWdtZW50LCBzdHJpcHBpbmcgb3V0IGFueSB1bnNhZmUgZWxlbWVudHMgYW5kIHVuc2FmZVxuICogYXR0cmlidXRlcy5cbiAqL1xuY2xhc3MgU2FuaXRpemluZ0h0bWxTZXJpYWxpemVyIHtcbiAgLy8gRXhwbGljaXRseSB0cmFjayBpZiBzb21ldGhpbmcgd2FzIHN0cmlwcGVkLCB0byBhdm9pZCBhY2NpZGVudGFsbHkgd2FybmluZyBvZiBzYW5pdGl6YXRpb24ganVzdFxuICAvLyBiZWNhdXNlIGNoYXJhY3RlcnMgd2VyZSByZS1lbmNvZGVkLlxuICBwdWJsaWMgc2FuaXRpemVkU29tZXRoaW5nID0gZmFsc2U7XG4gIHByaXZhdGUgYnVmOiBzdHJpbmdbXSA9IFtdO1xuXG4gIHNhbml0aXplQ2hpbGRyZW4oZWw6IEVsZW1lbnQpOiBzdHJpbmcge1xuICAgIC8vIFRoaXMgY2Fubm90IHVzZSBhIFRyZWVXYWxrZXIsIGFzIGl0IGhhcyB0byBydW4gb24gQW5ndWxhcidzIHZhcmlvdXMgRE9NIGFkYXB0ZXJzLlxuICAgIC8vIEhvd2V2ZXIgdGhpcyBjb2RlIG5ldmVyIGFjY2Vzc2VzIHByb3BlcnRpZXMgb2ZmIG9mIGBkb2N1bWVudGAgYmVmb3JlIGRlbGV0aW5nIGl0cyBjb250ZW50c1xuICAgIC8vIGFnYWluLCBzbyBpdCBzaG91bGRuJ3QgYmUgdnVsbmVyYWJsZSB0byBET00gY2xvYmJlcmluZy5cbiAgICBsZXQgY3VycmVudDogTm9kZSA9IGVsLmZpcnN0Q2hpbGQgITtcbiAgICBsZXQgdHJhdmVyc2VDb250ZW50ID0gdHJ1ZTtcbiAgICB3aGlsZSAoY3VycmVudCkge1xuICAgICAgaWYgKGN1cnJlbnQubm9kZVR5cGUgPT09IE5vZGUuRUxFTUVOVF9OT0RFKSB7XG4gICAgICAgIHRyYXZlcnNlQ29udGVudCA9IHRoaXMuc3RhcnRFbGVtZW50KGN1cnJlbnQgYXMgRWxlbWVudCk7XG4gICAgICB9IGVsc2UgaWYgKGN1cnJlbnQubm9kZVR5cGUgPT09IE5vZGUuVEVYVF9OT0RFKSB7XG4gICAgICAgIHRoaXMuY2hhcnMoY3VycmVudC5ub2RlVmFsdWUgISk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICAvLyBTdHJpcCBub24tZWxlbWVudCwgbm9uLXRleHQgbm9kZXMuXG4gICAgICAgIHRoaXMuc2FuaXRpemVkU29tZXRoaW5nID0gdHJ1ZTtcbiAgICAgIH1cbiAgICAgIGlmICh0cmF2ZXJzZUNvbnRlbnQgJiYgY3VycmVudC5maXJzdENoaWxkKSB7XG4gICAgICAgIGN1cnJlbnQgPSBjdXJyZW50LmZpcnN0Q2hpbGQgITtcbiAgICAgICAgY29udGludWU7XG4gICAgICB9XG4gICAgICB3aGlsZSAoY3VycmVudCkge1xuICAgICAgICAvLyBMZWF2aW5nIHRoZSBlbGVtZW50LiBXYWxrIHVwIGFuZCB0byB0aGUgcmlnaHQsIGNsb3NpbmcgdGFncyBhcyB3ZSBnby5cbiAgICAgICAgaWYgKGN1cnJlbnQubm9kZVR5cGUgPT09IE5vZGUuRUxFTUVOVF9OT0RFKSB7XG4gICAgICAgICAgdGhpcy5lbmRFbGVtZW50KGN1cnJlbnQgYXMgRWxlbWVudCk7XG4gICAgICAgIH1cblxuICAgICAgICBsZXQgbmV4dCA9IHRoaXMuY2hlY2tDbG9iYmVyZWRFbGVtZW50KGN1cnJlbnQsIGN1cnJlbnQubmV4dFNpYmxpbmcgISk7XG5cbiAgICAgICAgaWYgKG5leHQpIHtcbiAgICAgICAgICBjdXJyZW50ID0gbmV4dDtcbiAgICAgICAgICBicmVhaztcbiAgICAgICAgfVxuXG4gICAgICAgIGN1cnJlbnQgPSB0aGlzLmNoZWNrQ2xvYmJlcmVkRWxlbWVudChjdXJyZW50LCBjdXJyZW50LnBhcmVudE5vZGUgISk7XG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiB0aGlzLmJ1Zi5qb2luKCcnKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBTYW5pdGl6ZXMgYW4gb3BlbmluZyBlbGVtZW50IHRhZyAoaWYgdmFsaWQpIGFuZCByZXR1cm5zIHdoZXRoZXIgdGhlIGVsZW1lbnQncyBjb250ZW50cyBzaG91bGRcbiAgICogYmUgdHJhdmVyc2VkLiBFbGVtZW50IGNvbnRlbnQgbXVzdCBhbHdheXMgYmUgdHJhdmVyc2VkIChldmVuIGlmIHRoZSBlbGVtZW50IGl0c2VsZiBpcyBub3RcbiAgICogdmFsaWQvc2FmZSksIHVubGVzcyB0aGUgZWxlbWVudCBpcyBvbmUgb2YgYFNLSVBfVFJBVkVSU0lOR19DT05URU5UX0lGX0lOVkFMSURfRUxFTUVOVFNgLlxuICAgKlxuICAgKiBAcGFyYW0gZWxlbWVudCBUaGUgZWxlbWVudCB0byBzYW5pdGl6ZS5cbiAgICogQHJldHVybiBUcnVlIGlmIHRoZSBlbGVtZW50J3MgY29udGVudHMgc2hvdWxkIGJlIHRyYXZlcnNlZC5cbiAgICovXG4gIHByaXZhdGUgc3RhcnRFbGVtZW50KGVsZW1lbnQ6IEVsZW1lbnQpOiBib29sZWFuIHtcbiAgICBjb25zdCB0YWdOYW1lID0gZWxlbWVudC5ub2RlTmFtZS50b0xvd2VyQ2FzZSgpO1xuICAgIGlmICghVkFMSURfRUxFTUVOVFMuaGFzT3duUHJvcGVydHkodGFnTmFtZSkpIHtcbiAgICAgIHRoaXMuc2FuaXRpemVkU29tZXRoaW5nID0gdHJ1ZTtcbiAgICAgIHJldHVybiAhU0tJUF9UUkFWRVJTSU5HX0NPTlRFTlRfSUZfSU5WQUxJRF9FTEVNRU5UUy5oYXNPd25Qcm9wZXJ0eSh0YWdOYW1lKTtcbiAgICB9XG4gICAgdGhpcy5idWYucHVzaCgnPCcpO1xuICAgIHRoaXMuYnVmLnB1c2godGFnTmFtZSk7XG4gICAgY29uc3QgZWxBdHRycyA9IGVsZW1lbnQuYXR0cmlidXRlcztcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGVsQXR0cnMubGVuZ3RoOyBpKyspIHtcbiAgICAgIGNvbnN0IGVsQXR0ciA9IGVsQXR0cnMuaXRlbShpKTtcbiAgICAgIGNvbnN0IGF0dHJOYW1lID0gZWxBdHRyICEubmFtZTtcbiAgICAgIGNvbnN0IGxvd2VyID0gYXR0ck5hbWUudG9Mb3dlckNhc2UoKTtcbiAgICAgIGlmICghVkFMSURfQVRUUlMuaGFzT3duUHJvcGVydHkobG93ZXIpKSB7XG4gICAgICAgIHRoaXMuc2FuaXRpemVkU29tZXRoaW5nID0gdHJ1ZTtcbiAgICAgICAgY29udGludWU7XG4gICAgICB9XG4gICAgICBsZXQgdmFsdWUgPSBlbEF0dHIgIS52YWx1ZTtcbiAgICAgIC8vIFRPRE8obWFydGlucHJvYnN0KTogU3BlY2lhbCBjYXNlIGltYWdlIFVSSXMgZm9yIGRhdGE6aW1hZ2UvLi4uXG4gICAgICBpZiAoVVJJX0FUVFJTW2xvd2VyXSkgdmFsdWUgPSBfc2FuaXRpemVVcmwodmFsdWUpO1xuICAgICAgaWYgKFNSQ1NFVF9BVFRSU1tsb3dlcl0pIHZhbHVlID0gc2FuaXRpemVTcmNzZXQodmFsdWUpO1xuICAgICAgdGhpcy5idWYucHVzaCgnICcsIGF0dHJOYW1lLCAnPVwiJywgZW5jb2RlRW50aXRpZXModmFsdWUpLCAnXCInKTtcbiAgICB9XG4gICAgdGhpcy5idWYucHVzaCgnPicpO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG5cbiAgcHJpdmF0ZSBlbmRFbGVtZW50KGN1cnJlbnQ6IEVsZW1lbnQpIHtcbiAgICBjb25zdCB0YWdOYW1lID0gY3VycmVudC5ub2RlTmFtZS50b0xvd2VyQ2FzZSgpO1xuICAgIGlmIChWQUxJRF9FTEVNRU5UUy5oYXNPd25Qcm9wZXJ0eSh0YWdOYW1lKSAmJiAhVk9JRF9FTEVNRU5UUy5oYXNPd25Qcm9wZXJ0eSh0YWdOYW1lKSkge1xuICAgICAgdGhpcy5idWYucHVzaCgnPC8nKTtcbiAgICAgIHRoaXMuYnVmLnB1c2godGFnTmFtZSk7XG4gICAgICB0aGlzLmJ1Zi5wdXNoKCc+Jyk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBjaGFycyhjaGFyczogc3RyaW5nKSB7IHRoaXMuYnVmLnB1c2goZW5jb2RlRW50aXRpZXMoY2hhcnMpKTsgfVxuXG4gIGNoZWNrQ2xvYmJlcmVkRWxlbWVudChub2RlOiBOb2RlLCBuZXh0Tm9kZTogTm9kZSk6IE5vZGUge1xuICAgIGlmIChuZXh0Tm9kZSAmJlxuICAgICAgICAobm9kZS5jb21wYXJlRG9jdW1lbnRQb3NpdGlvbihuZXh0Tm9kZSkgJlxuICAgICAgICAgTm9kZS5ET0NVTUVOVF9QT1NJVElPTl9DT05UQUlORURfQlkpID09PcKgTm9kZS5ET0NVTUVOVF9QT1NJVElPTl9DT05UQUlORURfQlkpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICBgRmFpbGVkIHRvIHNhbml0aXplIGh0bWwgYmVjYXVzZSB0aGUgZWxlbWVudCBpcyBjbG9iYmVyZWQ6ICR7KG5vZGUgYXMgRWxlbWVudCkub3V0ZXJIVE1MfWApO1xuICAgIH1cbiAgICByZXR1cm4gbmV4dE5vZGU7XG4gIH1cbn1cblxuLy8gUmVndWxhciBFeHByZXNzaW9ucyBmb3IgcGFyc2luZyB0YWdzIGFuZCBhdHRyaWJ1dGVzXG5jb25zdCBTVVJST0dBVEVfUEFJUl9SRUdFWFAgPSAvW1xcdUQ4MDAtXFx1REJGRl1bXFx1REMwMC1cXHVERkZGXS9nO1xuLy8gISB0byB+IGlzIHRoZSBBU0NJSSByYW5nZS5cbmNvbnN0IE5PTl9BTFBIQU5VTUVSSUNfUkVHRVhQID0gLyhbXlxcIy1+IHwhXSkvZztcblxuLyoqXG4gKiBFc2NhcGVzIGFsbCBwb3RlbnRpYWxseSBkYW5nZXJvdXMgY2hhcmFjdGVycywgc28gdGhhdCB0aGVcbiAqIHJlc3VsdGluZyBzdHJpbmcgY2FuIGJlIHNhZmVseSBpbnNlcnRlZCBpbnRvIGF0dHJpYnV0ZSBvclxuICogZWxlbWVudCB0ZXh0LlxuICogQHBhcmFtIHZhbHVlXG4gKi9cbmZ1bmN0aW9uIGVuY29kZUVudGl0aWVzKHZhbHVlOiBzdHJpbmcpIHtcbiAgcmV0dXJuIHZhbHVlLnJlcGxhY2UoLyYvZywgJyZhbXA7JylcbiAgICAgIC5yZXBsYWNlKFxuICAgICAgICAgIFNVUlJPR0FURV9QQUlSX1JFR0VYUCxcbiAgICAgICAgICBmdW5jdGlvbihtYXRjaDogc3RyaW5nKSB7XG4gICAgICAgICAgICBjb25zdCBoaSA9IG1hdGNoLmNoYXJDb2RlQXQoMCk7XG4gICAgICAgICAgICBjb25zdCBsb3cgPSBtYXRjaC5jaGFyQ29kZUF0KDEpO1xuICAgICAgICAgICAgcmV0dXJuICcmIycgKyAoKChoaSAtIDB4RDgwMCkgKiAweDQwMCkgKyAobG93IC0gMHhEQzAwKSArIDB4MTAwMDApICsgJzsnO1xuICAgICAgICAgIH0pXG4gICAgICAucmVwbGFjZShcbiAgICAgICAgICBOT05fQUxQSEFOVU1FUklDX1JFR0VYUCxcbiAgICAgICAgICBmdW5jdGlvbihtYXRjaDogc3RyaW5nKSB7IHJldHVybiAnJiMnICsgbWF0Y2guY2hhckNvZGVBdCgwKSArICc7JzsgfSlcbiAgICAgIC5yZXBsYWNlKC88L2csICcmbHQ7JylcbiAgICAgIC5yZXBsYWNlKC8+L2csICcmZ3Q7Jyk7XG59XG5cbmxldCBpbmVydEJvZHlIZWxwZXI6IEluZXJ0Qm9keUhlbHBlcjtcblxuLyoqXG4gKiBTYW5pdGl6ZXMgdGhlIGdpdmVuIHVuc2FmZSwgdW50cnVzdGVkIEhUTUwgZnJhZ21lbnQsIGFuZCByZXR1cm5zIEhUTUwgdGV4dCB0aGF0IGlzIHNhZmUgdG8gYWRkIHRvXG4gKiB0aGUgRE9NIGluIGEgYnJvd3NlciBlbnZpcm9ubWVudC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIF9zYW5pdGl6ZUh0bWwoZGVmYXVsdERvYzogYW55LCB1bnNhZmVIdG1sSW5wdXQ6IHN0cmluZyk6IHN0cmluZyB7XG4gIGxldCBpbmVydEJvZHlFbGVtZW50OiBIVE1MRWxlbWVudHxudWxsID0gbnVsbDtcbiAgdHJ5IHtcbiAgICBpbmVydEJvZHlIZWxwZXIgPSBpbmVydEJvZHlIZWxwZXIgfHwgbmV3IEluZXJ0Qm9keUhlbHBlcihkZWZhdWx0RG9jKTtcbiAgICAvLyBNYWtlIHN1cmUgdW5zYWZlSHRtbCBpcyBhY3R1YWxseSBhIHN0cmluZyAoVHlwZVNjcmlwdCB0eXBlcyBhcmUgbm90IGVuZm9yY2VkIGF0IHJ1bnRpbWUpLlxuICAgIGxldCB1bnNhZmVIdG1sID0gdW5zYWZlSHRtbElucHV0ID8gU3RyaW5nKHVuc2FmZUh0bWxJbnB1dCkgOiAnJztcbiAgICBpbmVydEJvZHlFbGVtZW50ID0gaW5lcnRCb2R5SGVscGVyLmdldEluZXJ0Qm9keUVsZW1lbnQodW5zYWZlSHRtbCk7XG5cbiAgICAvLyBtWFNTIHByb3RlY3Rpb24uIFJlcGVhdGVkbHkgcGFyc2UgdGhlIGRvY3VtZW50IHRvIG1ha2Ugc3VyZSBpdCBzdGFiaWxpemVzLCBzbyB0aGF0IGEgYnJvd3NlclxuICAgIC8vIHRyeWluZyB0byBhdXRvLWNvcnJlY3QgaW5jb3JyZWN0IEhUTUwgY2Fubm90IGNhdXNlIGZvcm1lcmx5IGluZXJ0IEhUTUwgdG8gYmVjb21lIGRhbmdlcm91cy5cbiAgICBsZXQgbVhTU0F0dGVtcHRzID0gNTtcbiAgICBsZXQgcGFyc2VkSHRtbCA9IHVuc2FmZUh0bWw7XG5cbiAgICBkbyB7XG4gICAgICBpZiAobVhTU0F0dGVtcHRzID09PSAwKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcignRmFpbGVkIHRvIHNhbml0aXplIGh0bWwgYmVjYXVzZSB0aGUgaW5wdXQgaXMgdW5zdGFibGUnKTtcbiAgICAgIH1cbiAgICAgIG1YU1NBdHRlbXB0cy0tO1xuXG4gICAgICB1bnNhZmVIdG1sID0gcGFyc2VkSHRtbDtcbiAgICAgIHBhcnNlZEh0bWwgPSBpbmVydEJvZHlFbGVtZW50ICEuaW5uZXJIVE1MO1xuICAgICAgaW5lcnRCb2R5RWxlbWVudCA9IGluZXJ0Qm9keUhlbHBlci5nZXRJbmVydEJvZHlFbGVtZW50KHVuc2FmZUh0bWwpO1xuICAgIH0gd2hpbGUgKHVuc2FmZUh0bWwgIT09IHBhcnNlZEh0bWwpO1xuXG4gICAgY29uc3Qgc2FuaXRpemVyID0gbmV3IFNhbml0aXppbmdIdG1sU2VyaWFsaXplcigpO1xuICAgIGNvbnN0IHNhZmVIdG1sID0gc2FuaXRpemVyLnNhbml0aXplQ2hpbGRyZW4oXG4gICAgICAgIGdldFRlbXBsYXRlQ29udGVudChpbmVydEJvZHlFbGVtZW50ICEpIGFzIEVsZW1lbnQgfHwgaW5lcnRCb2R5RWxlbWVudCk7XG4gICAgaWYgKGlzRGV2TW9kZSgpICYmIHNhbml0aXplci5zYW5pdGl6ZWRTb21ldGhpbmcpIHtcbiAgICAgIGNvbnNvbGUud2FybihcbiAgICAgICAgICAnV0FSTklORzogc2FuaXRpemluZyBIVE1MIHN0cmlwcGVkIHNvbWUgY29udGVudCwgc2VlIGh0dHA6Ly9nLmNvL25nL3NlY3VyaXR5I3hzcycpO1xuICAgIH1cblxuICAgIHJldHVybiBzYWZlSHRtbDtcbiAgfSBmaW5hbGx5IHtcbiAgICAvLyBJbiBjYXNlIGFueXRoaW5nIGdvZXMgd3JvbmcsIGNsZWFyIG91dCBpbmVydEVsZW1lbnQgdG8gcmVzZXQgdGhlIGVudGlyZSBET00gc3RydWN0dXJlLlxuICAgIGlmIChpbmVydEJvZHlFbGVtZW50KSB7XG4gICAgICBjb25zdCBwYXJlbnQgPSBnZXRUZW1wbGF0ZUNvbnRlbnQoaW5lcnRCb2R5RWxlbWVudCkgfHwgaW5lcnRCb2R5RWxlbWVudDtcbiAgICAgIHdoaWxlIChwYXJlbnQuZmlyc3RDaGlsZCkge1xuICAgICAgICBwYXJlbnQucmVtb3ZlQ2hpbGQocGFyZW50LmZpcnN0Q2hpbGQpO1xuICAgICAgfVxuICAgIH1cbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0VGVtcGxhdGVDb250ZW50KGVsOiBOb2RlKTogTm9kZXxudWxsIHtcbiAgcmV0dXJuICdjb250ZW50JyBpbiAoZWwgYXMgYW55IC8qKiBNaWNyb3NvZnQvVHlwZVNjcmlwdCMyMTUxNyAqLykgJiYgaXNUZW1wbGF0ZUVsZW1lbnQoZWwpID9cbiAgICAgIGVsLmNvbnRlbnQgOlxuICAgICAgbnVsbDtcbn1cbmZ1bmN0aW9uIGlzVGVtcGxhdGVFbGVtZW50KGVsOiBOb2RlKTogZWwgaXMgSFRNTFRlbXBsYXRlRWxlbWVudCB7XG4gIHJldHVybiBlbC5ub2RlVHlwZSA9PT0gTm9kZS5FTEVNRU5UX05PREUgJiYgZWwubm9kZU5hbWUgPT09ICdURU1QTEFURSc7XG59XG4iXX0=