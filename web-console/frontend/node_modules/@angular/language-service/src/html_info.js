/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/language-service/src/html_info", ["require", "exports", "tslib"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var values = [
        'ID',
        'CDATA',
        'NAME',
        ['ltr', 'rtl'],
        ['rect', 'circle', 'poly', 'default'],
        'NUMBER',
        ['nohref'],
        ['ismap'],
        ['declare'],
        ['DATA', 'REF', 'OBJECT'],
        ['GET', 'POST'],
        'IDREF',
        ['TEXT', 'PASSWORD', 'CHECKBOX', 'RADIO', 'SUBMIT', 'RESET', 'FILE', 'HIDDEN', 'IMAGE', 'BUTTON'],
        ['checked'],
        ['disabled'],
        ['readonly'],
        ['multiple'],
        ['selected'],
        ['button', 'submit', 'reset'],
        ['void', 'above', 'below', 'hsides', 'lhs', 'rhs', 'vsides', 'box', 'border'],
        ['none', 'groups', 'rows', 'cols', 'all'],
        ['left', 'center', 'right', 'justify', 'char'],
        ['top', 'middle', 'bottom', 'baseline'],
        'IDREFS',
        ['row', 'col', 'rowgroup', 'colgroup'],
        ['defer']
    ];
    var groups = [
        { id: 0 },
        {
            onclick: 1,
            ondblclick: 1,
            onmousedown: 1,
            onmouseup: 1,
            onmouseover: 1,
            onmousemove: 1,
            onmouseout: 1,
            onkeypress: 1,
            onkeydown: 1,
            onkeyup: 1
        },
        { lang: 2, dir: 3 },
        { onload: 1, onunload: 1 },
        { name: 1 },
        { href: 1 },
        { type: 1 },
        { alt: 1 },
        { tabindex: 5 },
        { media: 1 },
        { nohref: 6 },
        { usemap: 1 },
        { src: 1 },
        { onfocus: 1, onblur: 1 },
        { charset: 1 },
        { declare: 8, classid: 1, codebase: 1, data: 1, codetype: 1, archive: 1, standby: 1 },
        { title: 1 },
        { value: 1 },
        { cite: 1 },
        { datetime: 1 },
        { accept: 1 },
        { shape: 4, coords: 1 },
        { for: 11
        },
        { action: 1, method: 10, enctype: 1, onsubmit: 1, onreset: 1, 'accept-charset': 1 },
        { valuetype: 9 },
        { longdesc: 1 },
        { width: 1 },
        { disabled: 14 },
        { readonly: 15, onselect: 1 },
        { accesskey: 1 },
        { size: 5, multiple: 16 },
        { onchange: 1 },
        { label: 1 },
        { selected: 17 },
        { type: 12, checked: 13, size: 1, maxlength: 5 },
        { rows: 5, cols: 5 },
        { type: 18 },
        { height: 1 },
        { summary: 1, border: 1, frame: 19, rules: 20, cellspacing: 1, cellpadding: 1, datapagesize: 1 },
        { align: 21, char: 1, charoff: 1, valign: 22 },
        { span: 5 },
        { abbr: 1, axis: 1, headers: 23, scope: 24, rowspan: 5, colspan: 5 },
        { profile: 1 },
        { 'http-equiv': 2, name: 2, content: 1, scheme: 1 },
        { class: 1, style: 1 },
        { hreflang: 2, rel: 1, rev: 1 },
        { ismap: 7 },
        { defer: 25, event: 1, for: 1 }
    ];
    var elements = {
        TT: [0, 1, 2, 16, 44],
        I: [0, 1, 2, 16, 44],
        B: [0, 1, 2, 16, 44],
        BIG: [0, 1, 2, 16, 44],
        SMALL: [0, 1, 2, 16, 44],
        EM: [0, 1, 2, 16, 44],
        STRONG: [0, 1, 2, 16, 44],
        DFN: [0, 1, 2, 16, 44],
        CODE: [0, 1, 2, 16, 44],
        SAMP: [0, 1, 2, 16, 44],
        KBD: [0, 1, 2, 16, 44],
        VAR: [0, 1, 2, 16, 44],
        CITE: [0, 1, 2, 16, 44],
        ABBR: [0, 1, 2, 16, 44],
        ACRONYM: [0, 1, 2, 16, 44],
        SUB: [0, 1, 2, 16, 44],
        SUP: [0, 1, 2, 16, 44],
        SPAN: [0, 1, 2, 16, 44],
        BDO: [0, 2, 16, 44],
        BR: [0, 16, 44],
        BODY: [0, 1, 2, 3, 16, 44],
        ADDRESS: [0, 1, 2, 16, 44],
        DIV: [0, 1, 2, 16, 44],
        A: [0, 1, 2, 4, 5, 6, 8, 13, 14, 16, 21, 29, 44, 45],
        MAP: [0, 1, 2, 4, 16, 44],
        AREA: [0, 1, 2, 5, 7, 8, 10, 13, 16, 21, 29, 44],
        LINK: [0, 1, 2, 5, 6, 9, 14, 16, 44, 45],
        IMG: [0, 1, 2, 4, 7, 11, 12, 16, 25, 26, 37, 44, 46],
        OBJECT: [0, 1, 2, 4, 6, 8, 11, 15, 16, 26, 37, 44],
        PARAM: [0, 4, 6, 17, 24],
        HR: [0, 1, 2, 16, 44],
        P: [0, 1, 2, 16, 44],
        H1: [0, 1, 2, 16, 44],
        H2: [0, 1, 2, 16, 44],
        H3: [0, 1, 2, 16, 44],
        H4: [0, 1, 2, 16, 44],
        H5: [0, 1, 2, 16, 44],
        H6: [0, 1, 2, 16, 44],
        PRE: [0, 1, 2, 16, 44],
        Q: [0, 1, 2, 16, 18, 44],
        BLOCKQUOTE: [0, 1, 2, 16, 18, 44],
        INS: [0, 1, 2, 16, 18, 19, 44],
        DEL: [0, 1, 2, 16, 18, 19, 44],
        DL: [0, 1, 2, 16, 44],
        DT: [0, 1, 2, 16, 44],
        DD: [0, 1, 2, 16, 44],
        OL: [0, 1, 2, 16, 44],
        UL: [0, 1, 2, 16, 44],
        LI: [0, 1, 2, 16, 44],
        FORM: [0, 1, 2, 4, 16, 20, 23, 44],
        LABEL: [0, 1, 2, 13, 16, 22, 29, 44],
        INPUT: [0, 1, 2, 4, 7, 8, 11, 12, 13, 16, 17, 20, 27, 28, 29, 31, 34, 44, 46],
        SELECT: [0, 1, 2, 4, 8, 13, 16, 27, 30, 31, 44],
        OPTGROUP: [0, 1, 2, 16, 27, 32, 44],
        OPTION: [0, 1, 2, 16, 17, 27, 32, 33, 44],
        TEXTAREA: [0, 1, 2, 4, 8, 13, 16, 27, 28, 29, 31, 35, 44],
        FIELDSET: [0, 1, 2, 16, 44],
        LEGEND: [0, 1, 2, 16, 29, 44],
        BUTTON: [0, 1, 2, 4, 8, 13, 16, 17, 27, 29, 36, 44],
        TABLE: [0, 1, 2, 16, 26, 38, 44],
        CAPTION: [0, 1, 2, 16, 44],
        COLGROUP: [0, 1, 2, 16, 26, 39, 40, 44],
        COL: [0, 1, 2, 16, 26, 39, 40, 44],
        THEAD: [0, 1, 2, 16, 39, 44],
        TBODY: [0, 1, 2, 16, 39, 44],
        TFOOT: [0, 1, 2, 16, 39, 44],
        TR: [0, 1, 2, 16, 39, 44],
        TH: [0, 1, 2, 16, 39, 41, 44],
        TD: [0, 1, 2, 16, 39, 41, 44],
        HEAD: [2, 42],
        TITLE: [2],
        BASE: [5],
        META: [2, 43],
        STYLE: [2, 6, 9, 16],
        SCRIPT: [6, 12, 14, 47],
        NOSCRIPT: [0, 1, 2, 16, 44],
        HTML: [2]
    };
    var defaultAttributes = [0, 1, 2, 4];
    function elementNames() {
        return Object.keys(elements).sort().map(function (v) { return v.toLowerCase(); });
    }
    exports.elementNames = elementNames;
    function compose(indexes) {
        var e_1, _a;
        var result = {};
        if (indexes) {
            try {
                for (var indexes_1 = tslib_1.__values(indexes), indexes_1_1 = indexes_1.next(); !indexes_1_1.done; indexes_1_1 = indexes_1.next()) {
                    var index = indexes_1_1.value;
                    var group = groups[index];
                    for (var name_1 in group)
                        if (group.hasOwnProperty(name_1))
                            result[name_1] = values[group[name_1]];
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (indexes_1_1 && !indexes_1_1.done && (_a = indexes_1.return)) _a.call(indexes_1);
                }
                finally { if (e_1) throw e_1.error; }
            }
        }
        return result;
    }
    function attributeNames(element) {
        return Object.keys(compose(elements[element.toUpperCase()] || defaultAttributes)).sort();
    }
    exports.attributeNames = attributeNames;
    function attributeType(element, attribute) {
        return compose(elements[element.toUpperCase()] || defaultAttributes)[attribute.toLowerCase()];
    }
    exports.attributeType = attributeType;
    // This section is describes the DOM property surface of a DOM element and is derivgulp formated
    // from
    // from the SCHEMA strings from the security context information. SCHEMA is copied here because
    // it would be an unnecessary risk to allow this array to be imported from the security context
    // schema registry.
    var SCHEMA = [
        '[Element]|textContent,%classList,className,id,innerHTML,*beforecopy,*beforecut,*beforepaste,*copy,*cut,*paste,*search,*selectstart,*webkitfullscreenchange,*webkitfullscreenerror,*wheel,outerHTML,#scrollLeft,#scrollTop,slot' +
            /* added manually to avoid breaking changes */
            ',*message,*mozfullscreenchange,*mozfullscreenerror,*mozpointerlockchange,*mozpointerlockerror,*webglcontextcreationerror,*webglcontextlost,*webglcontextrestored',
        '[HTMLElement]^[Element]|accessKey,contentEditable,dir,!draggable,!hidden,innerText,lang,*abort,*auxclick,*blur,*cancel,*canplay,*canplaythrough,*change,*click,*close,*contextmenu,*cuechange,*dblclick,*drag,*dragend,*dragenter,*dragleave,*dragover,*dragstart,*drop,*durationchange,*emptied,*ended,*error,*focus,*gotpointercapture,*input,*invalid,*keydown,*keypress,*keyup,*load,*loadeddata,*loadedmetadata,*loadstart,*lostpointercapture,*mousedown,*mouseenter,*mouseleave,*mousemove,*mouseout,*mouseover,*mouseup,*mousewheel,*pause,*play,*playing,*pointercancel,*pointerdown,*pointerenter,*pointerleave,*pointermove,*pointerout,*pointerover,*pointerup,*progress,*ratechange,*reset,*resize,*scroll,*seeked,*seeking,*select,*show,*stalled,*submit,*suspend,*timeupdate,*toggle,*volumechange,*waiting,outerText,!spellcheck,%style,#tabIndex,title,!translate',
        'abbr,address,article,aside,b,bdi,bdo,cite,code,dd,dfn,dt,em,figcaption,figure,footer,header,i,kbd,main,mark,nav,noscript,rb,rp,rt,rtc,ruby,s,samp,section,small,strong,sub,sup,u,var,wbr^[HTMLElement]|accessKey,contentEditable,dir,!draggable,!hidden,innerText,lang,*abort,*auxclick,*blur,*cancel,*canplay,*canplaythrough,*change,*click,*close,*contextmenu,*cuechange,*dblclick,*drag,*dragend,*dragenter,*dragleave,*dragover,*dragstart,*drop,*durationchange,*emptied,*ended,*error,*focus,*gotpointercapture,*input,*invalid,*keydown,*keypress,*keyup,*load,*loadeddata,*loadedmetadata,*loadstart,*lostpointercapture,*mousedown,*mouseenter,*mouseleave,*mousemove,*mouseout,*mouseover,*mouseup,*mousewheel,*pause,*play,*playing,*pointercancel,*pointerdown,*pointerenter,*pointerleave,*pointermove,*pointerout,*pointerover,*pointerup,*progress,*ratechange,*reset,*resize,*scroll,*seeked,*seeking,*select,*show,*stalled,*submit,*suspend,*timeupdate,*toggle,*volumechange,*waiting,outerText,!spellcheck,%style,#tabIndex,title,!translate',
        'media^[HTMLElement]|!autoplay,!controls,%controlsList,%crossOrigin,#currentTime,!defaultMuted,#defaultPlaybackRate,!disableRemotePlayback,!loop,!muted,*encrypted,*waitingforkey,#playbackRate,preload,src,%srcObject,#volume',
        ':svg:^[HTMLElement]|*abort,*auxclick,*blur,*cancel,*canplay,*canplaythrough,*change,*click,*close,*contextmenu,*cuechange,*dblclick,*drag,*dragend,*dragenter,*dragleave,*dragover,*dragstart,*drop,*durationchange,*emptied,*ended,*error,*focus,*gotpointercapture,*input,*invalid,*keydown,*keypress,*keyup,*load,*loadeddata,*loadedmetadata,*loadstart,*lostpointercapture,*mousedown,*mouseenter,*mouseleave,*mousemove,*mouseout,*mouseover,*mouseup,*mousewheel,*pause,*play,*playing,*pointercancel,*pointerdown,*pointerenter,*pointerleave,*pointermove,*pointerout,*pointerover,*pointerup,*progress,*ratechange,*reset,*resize,*scroll,*seeked,*seeking,*select,*show,*stalled,*submit,*suspend,*timeupdate,*toggle,*volumechange,*waiting,%style,#tabIndex',
        ':svg:graphics^:svg:|',
        ':svg:animation^:svg:|*begin,*end,*repeat',
        ':svg:geometry^:svg:|',
        ':svg:componentTransferFunction^:svg:|',
        ':svg:gradient^:svg:|',
        ':svg:textContent^:svg:graphics|',
        ':svg:textPositioning^:svg:textContent|',
        'a^[HTMLElement]|charset,coords,download,hash,host,hostname,href,hreflang,name,password,pathname,ping,port,protocol,referrerPolicy,rel,rev,search,shape,target,text,type,username',
        'area^[HTMLElement]|alt,coords,download,hash,host,hostname,href,!noHref,password,pathname,ping,port,protocol,referrerPolicy,rel,search,shape,target,username',
        'audio^media|',
        'br^[HTMLElement]|clear',
        'base^[HTMLElement]|href,target',
        'body^[HTMLElement]|aLink,background,bgColor,link,*beforeunload,*blur,*error,*focus,*hashchange,*languagechange,*load,*message,*offline,*online,*pagehide,*pageshow,*popstate,*rejectionhandled,*resize,*scroll,*storage,*unhandledrejection,*unload,text,vLink',
        'button^[HTMLElement]|!autofocus,!disabled,formAction,formEnctype,formMethod,!formNoValidate,formTarget,name,type,value',
        'canvas^[HTMLElement]|#height,#width',
        'content^[HTMLElement]|select',
        'dl^[HTMLElement]|!compact',
        'datalist^[HTMLElement]|',
        'details^[HTMLElement]|!open',
        'dialog^[HTMLElement]|!open,returnValue',
        'dir^[HTMLElement]|!compact',
        'div^[HTMLElement]|align',
        'embed^[HTMLElement]|align,height,name,src,type,width',
        'fieldset^[HTMLElement]|!disabled,name',
        'font^[HTMLElement]|color,face,size',
        'form^[HTMLElement]|acceptCharset,action,autocomplete,encoding,enctype,method,name,!noValidate,target',
        'frame^[HTMLElement]|frameBorder,longDesc,marginHeight,marginWidth,name,!noResize,scrolling,src',
        'frameset^[HTMLElement]|cols,*beforeunload,*blur,*error,*focus,*hashchange,*languagechange,*load,*message,*offline,*online,*pagehide,*pageshow,*popstate,*rejectionhandled,*resize,*scroll,*storage,*unhandledrejection,*unload,rows',
        'hr^[HTMLElement]|align,color,!noShade,size,width',
        'head^[HTMLElement]|',
        'h1,h2,h3,h4,h5,h6^[HTMLElement]|align',
        'html^[HTMLElement]|version',
        'iframe^[HTMLElement]|align,!allowFullscreen,frameBorder,height,longDesc,marginHeight,marginWidth,name,referrerPolicy,%sandbox,scrolling,src,srcdoc,width',
        'img^[HTMLElement]|align,alt,border,%crossOrigin,#height,#hspace,!isMap,longDesc,lowsrc,name,referrerPolicy,sizes,src,srcset,useMap,#vspace,#width',
        'input^[HTMLElement]|accept,align,alt,autocapitalize,autocomplete,!autofocus,!checked,!defaultChecked,defaultValue,dirName,!disabled,%files,formAction,formEnctype,formMethod,!formNoValidate,formTarget,#height,!incremental,!indeterminate,max,#maxLength,min,#minLength,!multiple,name,pattern,placeholder,!readOnly,!required,selectionDirection,#selectionEnd,#selectionStart,#size,src,step,type,useMap,value,%valueAsDate,#valueAsNumber,#width',
        'li^[HTMLElement]|type,#value',
        'label^[HTMLElement]|htmlFor',
        'legend^[HTMLElement]|align',
        'link^[HTMLElement]|as,charset,%crossOrigin,!disabled,href,hreflang,integrity,media,referrerPolicy,rel,%relList,rev,%sizes,target,type',
        'map^[HTMLElement]|name',
        'marquee^[HTMLElement]|behavior,bgColor,direction,height,#hspace,#loop,#scrollAmount,#scrollDelay,!trueSpeed,#vspace,width',
        'menu^[HTMLElement]|!compact',
        'meta^[HTMLElement]|content,httpEquiv,name,scheme',
        'meter^[HTMLElement]|#high,#low,#max,#min,#optimum,#value',
        'ins,del^[HTMLElement]|cite,dateTime',
        'ol^[HTMLElement]|!compact,!reversed,#start,type',
        'object^[HTMLElement]|align,archive,border,code,codeBase,codeType,data,!declare,height,#hspace,name,standby,type,useMap,#vspace,width',
        'optgroup^[HTMLElement]|!disabled,label',
        'option^[HTMLElement]|!defaultSelected,!disabled,label,!selected,text,value',
        'output^[HTMLElement]|defaultValue,%htmlFor,name,value',
        'p^[HTMLElement]|align',
        'param^[HTMLElement]|name,type,value,valueType',
        'picture^[HTMLElement]|',
        'pre^[HTMLElement]|#width',
        'progress^[HTMLElement]|#max,#value',
        'q,blockquote,cite^[HTMLElement]|',
        'script^[HTMLElement]|!async,charset,%crossOrigin,!defer,event,htmlFor,integrity,src,text,type',
        'select^[HTMLElement]|!autofocus,!disabled,#length,!multiple,name,!required,#selectedIndex,#size,value',
        'shadow^[HTMLElement]|',
        'slot^[HTMLElement]|name',
        'source^[HTMLElement]|media,sizes,src,srcset,type',
        'span^[HTMLElement]|',
        'style^[HTMLElement]|!disabled,media,type',
        'caption^[HTMLElement]|align',
        'th,td^[HTMLElement]|abbr,align,axis,bgColor,ch,chOff,#colSpan,headers,height,!noWrap,#rowSpan,scope,vAlign,width',
        'col,colgroup^[HTMLElement]|align,ch,chOff,#span,vAlign,width',
        'table^[HTMLElement]|align,bgColor,border,%caption,cellPadding,cellSpacing,frame,rules,summary,%tFoot,%tHead,width',
        'tr^[HTMLElement]|align,bgColor,ch,chOff,vAlign',
        'tfoot,thead,tbody^[HTMLElement]|align,ch,chOff,vAlign',
        'template^[HTMLElement]|',
        'textarea^[HTMLElement]|autocapitalize,!autofocus,#cols,defaultValue,dirName,!disabled,#maxLength,#minLength,name,placeholder,!readOnly,!required,#rows,selectionDirection,#selectionEnd,#selectionStart,value,wrap',
        'title^[HTMLElement]|text',
        'track^[HTMLElement]|!default,kind,label,src,srclang',
        'ul^[HTMLElement]|!compact,type',
        'unknown^[HTMLElement]|',
        'video^media|#height,poster,#width',
        ':svg:a^:svg:graphics|',
        ':svg:animate^:svg:animation|',
        ':svg:animateMotion^:svg:animation|',
        ':svg:animateTransform^:svg:animation|',
        ':svg:circle^:svg:geometry|',
        ':svg:clipPath^:svg:graphics|',
        ':svg:defs^:svg:graphics|',
        ':svg:desc^:svg:|',
        ':svg:discard^:svg:|',
        ':svg:ellipse^:svg:geometry|',
        ':svg:feBlend^:svg:|',
        ':svg:feColorMatrix^:svg:|',
        ':svg:feComponentTransfer^:svg:|',
        ':svg:feComposite^:svg:|',
        ':svg:feConvolveMatrix^:svg:|',
        ':svg:feDiffuseLighting^:svg:|',
        ':svg:feDisplacementMap^:svg:|',
        ':svg:feDistantLight^:svg:|',
        ':svg:feDropShadow^:svg:|',
        ':svg:feFlood^:svg:|',
        ':svg:feFuncA^:svg:componentTransferFunction|',
        ':svg:feFuncB^:svg:componentTransferFunction|',
        ':svg:feFuncG^:svg:componentTransferFunction|',
        ':svg:feFuncR^:svg:componentTransferFunction|',
        ':svg:feGaussianBlur^:svg:|',
        ':svg:feImage^:svg:|',
        ':svg:feMerge^:svg:|',
        ':svg:feMergeNode^:svg:|',
        ':svg:feMorphology^:svg:|',
        ':svg:feOffset^:svg:|',
        ':svg:fePointLight^:svg:|',
        ':svg:feSpecularLighting^:svg:|',
        ':svg:feSpotLight^:svg:|',
        ':svg:feTile^:svg:|',
        ':svg:feTurbulence^:svg:|',
        ':svg:filter^:svg:|',
        ':svg:foreignObject^:svg:graphics|',
        ':svg:g^:svg:graphics|',
        ':svg:image^:svg:graphics|',
        ':svg:line^:svg:geometry|',
        ':svg:linearGradient^:svg:gradient|',
        ':svg:mpath^:svg:|',
        ':svg:marker^:svg:|',
        ':svg:mask^:svg:|',
        ':svg:metadata^:svg:|',
        ':svg:path^:svg:geometry|',
        ':svg:pattern^:svg:|',
        ':svg:polygon^:svg:geometry|',
        ':svg:polyline^:svg:geometry|',
        ':svg:radialGradient^:svg:gradient|',
        ':svg:rect^:svg:geometry|',
        ':svg:svg^:svg:graphics|#currentScale,#zoomAndPan',
        ':svg:script^:svg:|type',
        ':svg:set^:svg:animation|',
        ':svg:stop^:svg:|',
        ':svg:style^:svg:|!disabled,media,title,type',
        ':svg:switch^:svg:graphics|',
        ':svg:symbol^:svg:|',
        ':svg:tspan^:svg:textPositioning|',
        ':svg:text^:svg:textPositioning|',
        ':svg:textPath^:svg:textContent|',
        ':svg:title^:svg:|',
        ':svg:use^:svg:graphics|',
        ':svg:view^:svg:|#zoomAndPan',
        'data^[HTMLElement]|value',
        'keygen^[HTMLElement]|!autofocus,challenge,!disabled,form,keytype,name',
        'menuitem^[HTMLElement]|type,label,icon,!disabled,!checked,radiogroup,!default',
        'summary^[HTMLElement]|',
        'time^[HTMLElement]|dateTime',
        ':svg:cursor^:svg:|',
    ];
    var attrToPropMap = {
        'class': 'className',
        'formaction': 'formAction',
        'innerHtml': 'innerHTML',
        'readonly': 'readOnly',
        'tabindex': 'tabIndex'
    };
    var EVENT = 'event';
    var BOOLEAN = 'boolean';
    var NUMBER = 'number';
    var STRING = 'string';
    var OBJECT = 'object';
    var SchemaInformation = /** @class */ (function () {
        function SchemaInformation() {
            var _this = this;
            this.schema = {};
            SCHEMA.forEach(function (encodedType) {
                var parts = encodedType.split('|');
                var properties = parts[1].split(',');
                var typeParts = (parts[0] + '^').split('^');
                var typeName = typeParts[0];
                var type = {};
                typeName.split(',').forEach(function (tag) { return _this.schema[tag.toLowerCase()] = type; });
                var superName = typeParts[1];
                var superType = superName && _this.schema[superName.toLowerCase()];
                if (superType) {
                    for (var key in superType) {
                        type[key] = superType[key];
                    }
                }
                properties.forEach(function (property) {
                    if (property == '') {
                    }
                    else if (property.startsWith('*')) {
                        type[property.substring(1)] = EVENT;
                    }
                    else if (property.startsWith('!')) {
                        type[property.substring(1)] = BOOLEAN;
                    }
                    else if (property.startsWith('#')) {
                        type[property.substring(1)] = NUMBER;
                    }
                    else if (property.startsWith('%')) {
                        type[property.substring(1)] = OBJECT;
                    }
                    else {
                        type[property] = STRING;
                    }
                });
            });
        }
        SchemaInformation.prototype.allKnownElements = function () { return Object.keys(this.schema); };
        SchemaInformation.prototype.eventsOf = function (elementName) {
            var elementType = this.schema[elementName.toLowerCase()] || {};
            return Object.keys(elementType).filter(function (property) { return elementType[property] === EVENT; });
        };
        SchemaInformation.prototype.propertiesOf = function (elementName) {
            var elementType = this.schema[elementName.toLowerCase()] || {};
            return Object.keys(elementType).filter(function (property) { return elementType[property] !== EVENT; });
        };
        SchemaInformation.prototype.typeOf = function (elementName, property) {
            return (this.schema[elementName.toLowerCase()] || {})[property];
        };
        Object.defineProperty(SchemaInformation, "instance", {
            get: function () {
                var result = SchemaInformation._instance;
                if (!result) {
                    result = SchemaInformation._instance = new SchemaInformation();
                }
                return result;
            },
            enumerable: true,
            configurable: true
        });
        return SchemaInformation;
    }());
    exports.SchemaInformation = SchemaInformation;
    function eventNames(elementName) {
        return SchemaInformation.instance.eventsOf(elementName);
    }
    exports.eventNames = eventNames;
    function propertyNames(elementName) {
        return SchemaInformation.instance.propertiesOf(elementName);
    }
    exports.propertyNames = propertyNames;
    function propertyType(elementName, propertyName) {
        return SchemaInformation.instance.typeOf(elementName, propertyName);
    }
    exports.propertyType = propertyType;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaHRtbF9pbmZvLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvbGFuZ3VhZ2Utc2VydmljZS9zcmMvaHRtbF9pbmZvLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7OztJQVdILElBQU0sTUFBTSxHQUFlO1FBQ3pCLElBQUk7UUFDSixPQUFPO1FBQ1AsTUFBTTtRQUNOLENBQUMsS0FBSyxFQUFFLEtBQUssQ0FBQztRQUNkLENBQUMsTUFBTSxFQUFFLFFBQVEsRUFBRSxNQUFNLEVBQUUsU0FBUyxDQUFDO1FBQ3JDLFFBQVE7UUFDUixDQUFDLFFBQVEsQ0FBQztRQUNWLENBQUMsT0FBTyxDQUFDO1FBQ1QsQ0FBQyxTQUFTLENBQUM7UUFDWCxDQUFDLE1BQU0sRUFBRSxLQUFLLEVBQUUsUUFBUSxDQUFDO1FBQ3pCLENBQUMsS0FBSyxFQUFFLE1BQU0sQ0FBQztRQUNmLE9BQU87UUFDUCxDQUFDLE1BQU0sRUFBRSxVQUFVLEVBQUUsVUFBVSxFQUFFLE9BQU8sRUFBRSxRQUFRLEVBQUUsT0FBTyxFQUFFLE1BQU0sRUFBRSxRQUFRLEVBQUUsT0FBTyxFQUFFLFFBQVEsQ0FBQztRQUNqRyxDQUFDLFNBQVMsQ0FBQztRQUNYLENBQUMsVUFBVSxDQUFDO1FBQ1osQ0FBQyxVQUFVLENBQUM7UUFDWixDQUFDLFVBQVUsQ0FBQztRQUNaLENBQUMsVUFBVSxDQUFDO1FBQ1osQ0FBQyxRQUFRLEVBQUUsUUFBUSxFQUFFLE9BQU8sQ0FBQztRQUM3QixDQUFDLE1BQU0sRUFBRSxPQUFPLEVBQUUsT0FBTyxFQUFFLFFBQVEsRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLFFBQVEsRUFBRSxLQUFLLEVBQUUsUUFBUSxDQUFDO1FBQzdFLENBQUMsTUFBTSxFQUFFLFFBQVEsRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLEtBQUssQ0FBQztRQUN6QyxDQUFDLE1BQU0sRUFBRSxRQUFRLEVB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QyxDQUFDO0lBRUYsSUFBTSxRQUFRLEdBQStCO1FBQzNDLEVBQUUsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDckIsQ0FBQyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUNwQixDQUFDLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3BCLEdBQUcsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDdEIsS0FBSyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUN4QixFQUFFLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3JCLE1BQU0sRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDekIsR0FBRyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUN0QixJQUFJLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3ZCLElBQUksRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDdkIsR0FBRyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUN0QixHQUFHLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3RCLElBQUksRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDdkIsSUFBSSxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUN2QixPQUFPLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQzFCLEdBQUcsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDdEIsR0FBRyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUN0QixJQUFJLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3ZCLEdBQUcsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUNuQixFQUFFLEVBQUUsQ0FBQyxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUNmLElBQUksRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQzFCLE9BQU8sRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDMUIsR0FBRyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUN0QixDQUFDLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3BELEdBQUcsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3pCLElBQUksRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ2hELElBQUksRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUN4QyxHQUFHLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDcEQsTUFBTSxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDbEQsS0FBSyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUN4QixFQUFFLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3JCLENBQUMsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDcEIsRUFBRSxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUNyQixFQUFFLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3JCLEVBQUUsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDckIsRUFBRSxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUNyQixFQUFFLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3JCLEVBQUUsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDckIsR0FBRyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUN0QixDQUFDLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUN4QixVQUFVLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUNqQyxHQUFHLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDOUIsR0FBRyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQzlCLEVBQUUsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDckIsRUFBRSxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUNyQixFQUFFLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3JCLEVBQUUsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDckIsRUFBRSxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUNyQixFQUFFLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3JCLElBQUksRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDbEMsS0FBSyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUNwQyxLQUFLLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDN0UsTUFBTSxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUMvQyxRQUFRLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDbkMsTUFBTSxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDekMsUUFBUSxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3pELFFBQVEsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDM0IsTUFBTSxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDN0IsTUFBTSxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDbkQsS0FBSyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ2hDLE9BQU8sRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDMUIsUUFBUSxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUN2QyxHQUFHLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ2xDLEtBQUssRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQzVCLEtBQUssRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQzVCLEtBQUssRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQzVCLEVBQUUsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3pCLEVBQUUsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQztRQUM3QixFQUFFLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUM7UUFDN0IsSUFBSSxFQUFFLENBQUMsQ0FBQyxFQUFFLEVBQUUsQ0FBQztRQUNiLEtBQUssRUFBRSxDQUFDLENBQUMsQ0FBQztRQUNWLElBQUksRUFBRSxDQUFDLENBQUMsQ0FBQztRQUNULElBQUksRUFBRSxDQUFDLENBQUMsRUFBRSxFQUFFLENBQUM7UUFDYixLQUFLLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxFQUFFLENBQUM7UUFDcEIsTUFBTSxFQUFFLENBQUMsQ0FBQyxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3ZCLFFBQVEsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxF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RUFBL0IsQ0FBK0IsQ0FBQyxDQUFDO1FBQ3RGLENBQUM7UUFFRCxrQ0FBTSxHQUFOLFVBQU8sV0FBbUIsRUFBRSxRQUFnQjtZQUMxQyxPQUFPLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxXQUFXLENBQUMsV0FBVyxFQUFFLENBQUMsSUFBSSxFQUFFLENBQUMsQ0FBQyxRQUFRLENBQUMsQ0FBQztRQUNsRSxDQUFDO1FBSUQsc0JBQVcsNkJBQVE7aUJBQW5CO2dCQUNFLElBQUksTUFBTSxHQUFHLGlCQUFpQixDQUFDLFNBQVMsQ0FBQztnQkFDekMsSUFBSSxDQUFDLE1BQU0sRUFBRTtvQkFDWCxNQUFNLEdBQUcsaUJBQWlCLENBQUMsU0FBUyxHQUFHLElBQUksaUJBQWlCLEVBQUUsQ0FBQztpQkFDaEU7Z0JBQ0QsT0FBTyxNQUFNLENBQUM7WUFDaEIsQ0FBQzs7O1dBQUE7UUFDSCx3QkFBQztJQUFELENBQUMsQUE1REQsSUE0REM7SUE1RFksOENBQWlCO0lBOEQ5QixTQUFnQixVQUFVLENBQUMsV0FBbUI7UUFDNUMsT0FBTyxpQkFBaUIsQ0FBQyxRQUFRLENBQUMsUUFBUSxDQUFDLFdBQVcsQ0FBQyxDQUFDO0lBQzFELENBQUM7SUFGRCxnQ0FFQztJQUVELFNBQWdCLGFBQWEsQ0FBQyxXQUFtQjtRQUMvQyxPQUFPLGlCQUFpQixDQUFDLFFBQVEsQ0FBQyxZQUFZLENBQUMsV0FBVyxDQUFDLENBQUM7SUFDOUQsQ0FBQztJQUZELHNDQUVDO0lBRUQsU0FBZ0IsWUFBWSxDQUFDLFdBQW1CLEVBQUUsWUFBb0I7UUFDcEUsT0FBTyxpQkFBaUIsQ0FBQyxRQUFRLENBQUMsTUFBTSxDQUFDLFdBQVcsRUFBRSxZQUFZLENBQUMsQ0FBQztJQUN0RSxDQUFDO0lBRkQsb0NBRUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8vIEluZm9ybWF0aW9uIGFib3V0IHRoZSBIVE1MIERPTSBlbGVtZW50c1xuXG4vLyBUaGlzIHNlY3Rpb24gZGVmaW5lcyB0aGUgSFRNTCBlbGVtZW50cyBhbmQgYXR0cmlidXRlIHN1cmZhY2Ugb2YgSFRNTCA0XG4vLyB3aGljaCBpcyBkZXJpdmVkIGZyb20gaHR0cHM6Ly93d3cudzMub3JnL1RSL2h0bWw0L3N0cmljdC5kdGRcbnR5cGUgYXR0clR5cGUgPSBzdHJpbmcgfCBzdHJpbmdbXTtcbnR5cGUgaGFzaDxUPiA9IHtcbiAgW25hbWU6IHN0cmluZ106IFRcbn07XG5cbmNvbnN0IHZhbHVlczogYXR0clR5cGVbXSA9IFtcbiAgJ0lEJyxcbiAgJ0NEQVRBJyxcbiAgJ05BTUUnLFxuICBbJ2x0cicsICdydGwnXSxcbiAgWydyZWN0JywgJ2NpcmNsZScsICdwb2x5JywgJ2RlZmF1bHQnXSxcbiAgJ05VTUJFUicsXG4gIFsnbm9ocmVmJ10sXG4gIFsnaXNtYXAnXSxcbiAgWydkZWNsYXJlJ10sXG4gIFsnREFUQScsICdSRUYnLCAnT0JKRUNUJ10sXG4gIFsnR0VUJywgJ1BPU1QnXSxcbiAgJ0lEUkVGJyxcbiAgWydURVhUJywgJ1BBU1NXT1JEJywgJ0NIRUNLQk9YJywgJ1JBRElPJywgJ1NVQk1JVCcsICdSRVNFVCcsICdGSUxFJywgJ0hJRERFTicsICdJTUFHRScsICdCVVRUT04nXSxcbiAgWydjaGVja2VkJ10sXG4gIFsnZGlzYWJsZWQnXSxcbiAgWydyZWFkb25seSddLFxuICBbJ211bHRpcGxlJ10sXG4gIFsnc2VsZWN0ZWQnXSxcbiAgWydidXR0b24nLCAnc3VibWl0JywgJ3Jlc2V0J10sXG4gIFsndm9pZCcsICdhYm92ZScsICdiZWxvdycsICdoc2lkZXMnLCAnbGhzJywgJ3JocycsICd2c2lkZXMnLCAnYm94JywgJ2JvcmRlciddLFxuICBbJ25vbmUnLCAnZ3JvdXBzJywgJ3Jvd3MnLCAnY29scycsICdhbGwnXSxcbiAgWydsZWZ0JywgJ2NlbnRlcicsICdyaWdodCcsICdqdXN0aWZ5JywgJ2NoYXInXSxcbiAgWyd0b3AnLCAnbWlkZGxlJywgJ2JvdHRvbScsICdiYXNlbGluZSddLFxuICAnSURSRUZTJyxcbiAgWydyb3cnLCAnY29sJywgJ3Jvd2dyb3VwJywgJ2NvbGdyb3VwJ10sXG4gIFsnZGVmZXInXVxuXTtcblxuY29uc3QgZ3JvdXBzOiBoYXNoPG51bWJlcj5bXSA9IFtcbiAge2lkOiAwfSxcbiAge1xuICAgIG9uY2xpY2s6IDEsXG4gICAgb25kYmxjbGljazogMSxcbiAgICBvbm1vdXNlZG93bjogMSxcbiAgICBvbm1vdXNldXA6IDEsXG4gICAgb25tb3VzZW92ZXI6IDEsXG4gICAgb25tb3VzZW1vdmU6IDEsXG4gICAgb25tb3VzZW91dDogMSxcbiAgICBvbmtleXByZXNzOiAxLFxuICAgIG9ua2V5ZG93bjogMSxcbiAgICBvbmtleXVwOiAxXG4gIH0sXG4gIHtsYW5nOiAyLCBkaXI6IDN9LFxuICB7b25sb2FkOiAxLCBvbnVubG9hZDogMX0sXG4gIHtuYW1lOiAxfSxcbiAge2hyZWY6IDF9LFxuICB7dHlwZTogMX0sXG4gIHthbHQ6IDF9LFxuICB7dGFiaW5kZXg6IDV9LFxuICB7bWVkaWE6IDF9LFxuICB7bm9ocmVmOiA2fSxcbiAge3VzZW1hcDogMX0sXG4gIHtzcmM6IDF9LFxuICB7b25mb2N1czogMSwgb25ibHVyOiAxfSxcbiAge2NoYXJzZXQ6IDF9LFxuICB7ZGVjbGFyZTogOCwgY2xhc3NpZDogMSwgY29kZWJhc2U6IDEsIGRhdGE6IDEsIGNvZGV0eXBlOiAxLCBhcmNoaXZlOiAxLCBzdGFuZGJ5OiAxfSxcbiAge3RpdGxlOiAxfSxcbiAge3ZhbHVlOiAxfSxcbiAge2NpdGU6IDF9LFxuICB7ZGF0ZXRpbWU6IDF9LFxuICB7YWNjZXB0OiAxfSxcbiAge3NoYXBlOiA0LCBjb29yZHM6IDF9LFxuICB7IGZvcjogMTFcbiAgfSxcbiAge2FjdGlvbjogMSwgbWV0aG9kOiAxMCwgZW5jdHlwZTogMSwgb25zdWJtaXQ6IDEsIG9ucmVzZXQ6IDEsICdhY2NlcHQtY2hhcnNldCc6IDF9LFxuICB7dmFsdWV0eXBlOiA5fSxcbiAge2xvbmdkZXNjOiAxfSxcbiAge3dpZHRoOiAxfSxcbiAge2Rpc2FibGVkOiAxNH0sXG4gIHtyZWFkb25seTogMTUsIG9uc2VsZWN0OiAxfSxcbiAge2FjY2Vzc2tleTogMX0sXG4gIHtzaXplOiA1LCBtdWx0aXBsZTogMTZ9LFxuICB7b25jaGFuZ2U6IDF9LFxuICB7bGFiZWw6IDF9LFxuICB7c2VsZWN0ZWQ6IDE3fSxcbiAge3R5cGU6IDEyLCBjaGVja2VkOiAxMywgc2l6ZTogMSwgbWF4bGVuZ3RoOiA1fSxcbiAge3Jvd3M6IDUsIGNvbHM6IDV9LFxuICB7dHlwZTogMTh9LFxuICB7aGVpZ2h0OiAxfSxcbiAge3N1bW1hcnk6IDEsIGJvcmRlcjogMSwgZnJhbWU6IDE5LCBydWxlczogMjAsIGNlbGxzcGFjaW5nOiAxLCBjZWxscGFkZGluZzogMSwgZGF0YXBhZ2VzaXplOiAxfSxcbiAge2FsaWduOiAyMSwgY2hhcjogMSwgY2hhcm9mZjogMSwgdmFsaWduOiAyMn0sXG4gIHtzcGFuOiA1fSxcbiAge2FiYnI6IDEsIGF4aXM6IDEsIGhlYWRlcnM6IDIzLCBzY29wZTogMjQsIHJvd3NwYW46IDUsIGNvbHNwYW46IDV9LFxuICB7cHJvZmlsZTogMX0sXG4gIHsnaHR0cC1lcXVpdic6IDIsIG5hbWU6IDIsIGNvbnRlbnQ6IDEsIHNjaGVtZTogMX0sXG4gIHtjbGFzczogMSwgc3R5bGU6IDF9LFxuICB7aHJlZmxhbmc6IDIsIHJlbDogMSwgcmV2OiAxfSxcbiAge2lzbWFwOiA3fSxcbiAgeyBkZWZlcjogMjUsIGV2ZW50OiAxLCBmb3IgOiAxIH1cbl07XG5cbmNvbnN0IGVsZW1lbnRzOiB7W25hbWU6IHN0cmluZ106IG51bWJlcltdfSA9IHtcbiAgVFQ6IFswLCAxLCAyLCAxNiwgNDRdLFxuICBJOiBbMCwgMSwgMiwgMTYsIDQ0XSxcbiAgQjogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIEJJRzogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIFNNQUxMOiBbMCwgMSwgMiwgMTYsIDQ0XSxcbiAgRU06IFswLCAxLCAyLCAxNiwgNDRdLFxuICBTVFJPTkc6IFswLCAxLCAyLCAxNiwgNDRdLFxuICBERk46IFswLCAxLCAyLCAxNiwgNDRdLFxuICBDT0RFOiBbMCwgMSwgMiwgMTYsIDQ0XSxcbiAgU0FNUDogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIEtCRDogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIFZBUjogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIENJVEU6IFswLCAxLCAyLCAxNiwgNDRdLFxuICBBQkJSOiBbMCwgMSwgMiwgMTYsIDQ0XSxcbiAgQUNST05ZTTogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIFNVQjogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIFNVUDogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIFNQQU46IFswLCAxLCAyLCAxNiwgNDRdLFxuICBCRE86IFswLCAyLCAxNiwgNDRdLFxuICBCUjogWzAsIDE2LCA0NF0sXG4gIEJPRFk6IFswLCAxLCAyLCAzLCAxNiwgNDRdLFxuICBBRERSRVNTOiBbMCwgMSwgMiwgMTYsIDQ0XSxcbiAgRElWOiBbMCwgMSwgMiwgMTYsIDQ0XSxcbiAgQTogWzAsIDEsIDIsIDQsIDUsIDYsIDgsIDEzLCAxNCwgMTYsIDIxLCAyOSwgNDQsIDQ1XSxcbiAgTUFQOiBbMCwgMSwgMiwgNCwgMTYsIDQ0XSxcbiAgQVJFQTogWzAsIDEsIDIsIDUsIDcsIDgsIDEwLCAxMywgMTYsIDIxLCAyOSwgNDRdLFxuICBMSU5LOiBbMCwgMSwgMiwgNSwgNiwgOSwgMTQsIDE2LCA0NCwgNDVdLFxuICBJTUc6IFswLCAxLCAyLCA0LCA3LCAxMSwgMTIsIDE2LCAyNSwgMjYsIDM3LCA0NCwgNDZdLFxuICBPQkpFQ1Q6IFswLCAxLCAyLCA0LCA2LCA4LCAxMSwgMTUsIDE2LCAyNiwgMzcsIDQ0XSxcbiAgUEFSQU06IFswLCA0LCA2LCAxNywgMjRdLFxuICBIUjogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIFA6IFswLCAxLCAyLCAxNiwgNDRdLFxuICBIMTogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIEgyOiBbMCwgMSwgMiwgMTYsIDQ0XSxcbiAgSDM6IFswLCAxLCAyLCAxNiwgNDRdLFxuICBINDogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIEg1OiBbMCwgMSwgMiwgMTYsIDQ0XSxcbiAgSDY6IFswLCAxLCAyLCAxNiwgNDRdLFxuICBQUkU6IFswLCAxLCAyLCAxNiwgNDRdLFxuICBROiBbMCwgMSwgMiwgMTYsIDE4LCA0NF0sXG4gIEJMT0NLUVVPVEU6IFswLCAxLCAyLCAxNiwgMTgsIDQ0XSxcbiAgSU5TOiBbMCwgMSwgMiwgMTYsIDE4LCAxOSwgNDRdLFxuICBERUw6IFswLCAxLCAyLCAxNiwgMTgsIDE5LCA0NF0sXG4gIERMOiBbMCwgMSwgMiwgMTYsIDQ0XSxcbiAgRFQ6IFswLCAxLCAyLCAxNiwgNDRdLFxuICBERDogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIE9MOiBbMCwgMSwgMiwgMTYsIDQ0XSxcbiAgVUw6IFswLCAxLCAyLCAxNiwgNDRdLFxuICBMSTogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIEZPUk06IFswLCAxLCAyLCA0LCAxNiwgMjAsIDIzLCA0NF0sXG4gIExBQkVMOiBbMCwgMSwgMiwgMTMsIDE2LCAyMiwgMjksIDQ0XSxcbiAgSU5QVVQ6IFswLCAxLCAyLCA0LCA3LCA4LCAxMSwgMTIsIDEzLCAxNiwgMTcsIDIwLCAyNywgMjgsIDI5LCAzMSwgMzQsIDQ0LCA0Nl0sXG4gIFNFTEVDVDogWzAsIDEsIDIsIDQsIDgsIDEzLCAxNiwgMjcsIDMwLCAzMSwgNDRdLFxuICBPUFRHUk9VUDogWzAsIDEsIDIsIDE2LCAyNywgMzIsIDQ0XSxcbiAgT1BUSU9OOiBbMCwgMSwgMiwgMTYsIDE3LCAyNywgMzIsIDMzLCA0NF0sXG4gIFRFWFRBUkVBOiBbMCwgMSwgMiwgNCwgOCwgMTMsIDE2LCAyNywgMjgsIDI5LCAzMSwgMzUsIDQ0XSxcbiAgRklFTERTRVQ6IFswLCAxLCAyLCAxNiwgNDRdLFxuICBMRUdFTkQ6IFswLCAxLCAyLCAxNiwgMjksIDQ0XSxcbiAgQlVUVE9OOiBbMCwgMSwgMiwgNCwgOCwgMTMsIDE2LCAxNywgMjcsIDI5LCAzNiwgNDRdLFxuICBUQUJMRTogWzAsIDEsIDIsIDE2LCAyNiwgMzgsIDQ0XSxcbiAgQ0FQVElPTjogWzAsIDEsIDIsIDE2LCA0NF0sXG4gIENPTEdST1VQOiBbMCwgMSwgMiwgMTYsIDI2LCAzOSwgNDAsIDQ0XSxcbiAgQ09MOiBbMCwgMSwgMiwgMTYsIDI2LCAzOSwgNDAsIDQ0XSxcbiAgVEhFQUQ6IFswLCAxLCAyLCAxNiwgMzksIDQ0XSxcbiAgVEJPRFk6IFswLCAxLCAyLCAxNiwgMzksIDQ0XSxcbiAgVEZPT1Q6IFswLCAxLCAyLCAxNiwgMzksIDQ0XSxcbiAgVFI6IFswLCAxLCAyLCAxNiwgMzksIDQ0XSxcbiAgVEg6IFswLCAxLCAyLCAxNiwgMzksIDQxLCA0NF0sXG4gIFREOiBbMCwgMSwgMiwgMTYsIDM5LCA0MSwgNDRdLFxuICBIRUFEOiBbMiwgNDJdLFxuICBUSVRMRTogWzJdLFxuICBCQVNFOiBbNV0sXG4gIE1FVEE6IFsyLCA0M10sXG4gIFNUWUxFOiBbMiwgNiwgOSwgMTZdLFxuICBTQ1JJUFQ6IFs2LCAxMiwgMTQsIDQ3XSxcbiAgTk9TQ1JJUFQ6IFswLCAxLCAyLCAxNiwgNDRdLFxuICBIVE1MOiBbMl1cbn07XG5cbmNvbnN0IGRlZmF1bHRBdHRyaWJ1dGVzID0gWzAsIDEsIDIsIDRdO1xuXG5leHBvcnQgZnVuY3Rpb24gZWxlbWVudE5hbWVzKCk6IHN0cmluZ1tdIHtcbiAgcmV0dXJuIE9iamVjdC5rZXlzKGVsZW1lbnRzKS5zb3J0KCkubWFwKHYgPT4gdi50b0xvd2VyQ2FzZSgpKTtcbn1cblxuZnVuY3Rpb24gY29tcG9zZShpbmRleGVzOiBudW1iZXJbXSB8IHVuZGVmaW5lZCk6IGhhc2g8YXR0clR5cGU+IHtcbiAgY29uc3QgcmVzdWx0OiBoYXNoPGF0dHJUeXBlPiA9IHt9O1xuICBpZiAoaW5kZXhlcykge1xuICAgIGZvciAobGV0IGluZGV4IG9mIGluZGV4ZXMpIHtcbiAgICAgIGNvbnN0IGdyb3VwID0gZ3JvdXBzW2luZGV4XTtcbiAgICAgIGZvciAobGV0IG5hbWUgaW4gZ3JvdXApXG4gICAgICAgIGlmIChncm91cC5oYXNPd25Qcm9wZXJ0eShuYW1lKSkgcmVzdWx0W25hbWVdID0gdmFsdWVzW2dyb3VwW25hbWVdXTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIHJlc3VsdDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGF0dHJpYnV0ZU5hbWVzKGVsZW1lbnQ6IHN0cmluZyk6IHN0cmluZ1tdIHtcbiAgcmV0dXJuIE9iamVjdC5rZXlzKGNvbXBvc2UoZWxlbWVudHNbZWxlbWVudC50b1VwcGVyQ2FzZSgpXSB8fCBkZWZhdWx0QXR0cmlidXRlcykpLnNvcnQoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGF0dHJpYnV0ZVR5cGUoZWxlbWVudDogc3RyaW5nLCBhdHRyaWJ1dGU6IHN0cmluZyk6IHN0cmluZ3xzdHJpbmdbXXx1bmRlZmluZWQge1xuICByZXR1cm4gY29tcG9zZShlbGVtZW50c1tlbGVtZW50LnRvVXBwZXJDYXNlKCldIHx8IGRlZmF1bHRBdHRyaWJ1dGVzKVthdHRyaWJ1dGUudG9Mb3dlckNhc2UoKV07XG59XG5cbi8vIFRoaXMgc2VjdGlvbiBpcyBkZXNjcmliZXMgdGhlIERPTSBwcm9wZXJ0eSBzdXJmYWNlIG9mIGEgRE9NIGVsZW1lbnQgYW5kIGlzIGRlcml2Z3VscCBmb3JtYXRlZFxuLy8gZnJvbVxuLy8gZnJvbSB0aGUgU0NIRU1BIHN0cmluZ3MgZnJvbSB0aGUgc2VjdXJpdHkgY29udGV4dCBpbmZvcm1hdGlvbi4gU0NIRU1BIGlzIGNvcGllZCBoZXJlIGJlY2F1c2Vcbi8vIGl0IHdvdWxkIGJlIGFuIHVubmVjZXNzYXJ5IHJpc2sgdG8gYWxsb3cgdGhpcyBhcnJheSB0byBiZSBpbXBvcnRlZCBmcm9tIHRoZSBzZWN1cml0eSBjb250ZXh0XG4vLyBzY2hlbWEgcmVnaXN0cnkuXG5jb25zdCBTQ0hFTUE6IHN0cmluZ1tdID0gW1xuICAnW0VsZW1lbnRdfHRleHRDb250ZW50LCVjbGFzc0xpc3QsY2xhc3NOYW1lLGlkLGlubmVySFRNTCwqYmVmb3JlY29weSwqYmVmb3JlY3V0LCpiZWZvcmVwYXN0ZSwqY29weSwqY3V0LCpwYXN0ZSwqc2VhcmNoLCpzZWxlY3RzdGFydCwqd2Via2l0ZnVsbHNjcmVlbmNoYW5nZSwqd2Via2l0ZnVsbHNjcmVlbmVycm9yLCp3aGVlbCxvdXRlckhUTUwsI3Njcm9sbExlZnQsI3Njcm9sbFRvcCxzbG90JyArXG4gICAgICAvKiBhZGRlZCBtYW51YWxseSB0byBhdm9pZCBicmVha2luZyBjaGFuZ2VzICovXG4gICAgICAnLCptZXNzYWdlLCptb3pmdWxsc2NyZWVuY2hhbmdlLCptb3pmdWxsc2NyZWVuZXJyb3IsKm1venBvaW50ZXJsb2NrY2hhbmdlLCptb3pwb2ludGVybG9ja2Vycm9yLCp3ZWJnbGNvbnRleHRjcmVhdGlvbmVycm9yLCp3ZWJnbGNvbnRleHRsb3N0LCp3ZWJnbGNvbnRleHRyZXN0b3JlZCcsXG4gICdbSFRNTEVsZW1lbnRdXltFbGVtZW50XXxhY2Nlc3NLZXksY29udGVudEVkaXRhYmxlLGRpciwhZHJhZ2dhYmxlLCFoaWRkZW4saW5uZXJUZXh0LGxhbmcsKmFib3J0LCphdXhjbGljaywqYmx1ciwqY2FuY2VsLCpjYW5wbGF5LCpjYW5wbGF5dGhyb3VnaCwqY2hhbmdlLCpjbGljaywqY2xvc2UsKmNvbnRleHRtZW51LCpjdWVjaGFuZ2UsKmRibGNsaWNrLCpkcmFnLCpkcmFnZW5kLCpkcmFnZW50ZXIsKmRyYWdsZWF2ZSwqZHJhZ292ZXIsKmRyYWdzdGFydCwqZHJvcCwqZHVyYXRpb25jaGFuZ2UsKmVtcHRpZWQsKmVuZGVkLCplcnJvciwqZm9jdXMsKmdvdHBvaW50ZXJjYXB0dXJlLCppbnB1dCwqaW52YWxpZCwqa2V5ZG93biwqa2V5cHJlc3MsKmtleXVwLCpsb2FkLCpsb2FkZWRkYXRhLCpsb2FkZWRtZXRhZGF0YSwqbG9hZHN0YXJ0LCpsb3N0cG9pbnRlcmNhcHR1cmUsKm1vdXNlZG93biwqbW91c2VlbnRlciwqbW91c2VsZWF2ZSwqbW91c2Vtb3ZlLCptb3VzZW91dCwqbW91c2VvdmVyLCptb3VzZXVwLCptb3VzZXdoZWVsLCpwYXVzZSwqcGxheSwqcGxheWluZywqcG9pbnRlcmNhbmNlbCwqcG9pbnRlcmRvd24sKnBvaW50ZXJlbnRlciwqcG9pbnRlcmxlYXZlLCpwb2ludGVybW92ZSwqcG9pbnRlcm91dCwqcG9pbnRlcm92ZXIsKnBvaW50ZXJ1cCwqcHJvZ3Jlc3MsKnJhdGVjaGFuZ2UsKnJlc2V0LCpyZXNpemUsKnNjcm9sbCwqc2Vla2VkLCpzZWVraW5nLCpzZWxlY3QsKnNob3csKnN0YWxsZWQsKnN1Ym1pdCwqc3VzcGVuZCwqdGltZXVwZGF0ZSwqdG9nZ2xlLCp2b2x1bWVjaGFuZ2UsKndhaXRpbmcsb3V0ZXJUZXh0LCFzcGVsbGNoZWNrLCVzdHlsZSwjdGFiSW5kZXgsdGl0bGUsIXRyYW5zbGF0ZScsXG4gICdhYmJyLGFkZHJlc3MsYXJ0aWNsZSxhc2lkZSxiLGJkaSxiZG8sY2l0ZSxjb2RlLGRkLGRmbixkdCxlbSxmaWdjYXB0aW9uLGZpZ3VyZSxmb290ZXIsaGVhZGVyLGksa2JkLG1haW4sbWFyayxuYXYsbm9zY3JpcHQscmIscnAscnQscnRjLHJ1YnkscyxzYW1wLHNlY3Rpb24sc21hbGwsc3Ryb25nLHN1YixzdXAsdSx2YXIsd2JyXltIVE1MRWxlbWVudF18YWNjZXNzS2V5LGNvbnRlbnRFZGl0YWJsZSxkaXIsIWRyYWdnYWJsZSwhaGlkZGVuLGlubmVyVGV4dCxsYW5nLCphYm9ydCwqYXV4Y2xpY2ssKmJsdXIsKmNhbmNlbCwqY2FucGxheSwqY2FucGxheXRocm91Z2gsKmNoYW5nZSwqY2xpY2ssKmNsb3NlLCpjb250ZXh0bWVudSwqY3VlY2hhbmdlLCpkYmxjbGljaywqZHJhZywqZHJhZ2VuZCwqZHJhZ2VudGVyLCpkcmFnbGVhdmUsKmRyYWdvdmVyLCpkcmFnc3RhcnQsKmRyb3AsKmR1cmF0aW9uY2hhbmdlLCplbXB0aWVkLCplbmRlZCwqZXJyb3IsKmZvY3VzLCpnb3Rwb2ludGVyY2FwdHVyZSwqaW5wdXQsKmludmFsaWQsKmtleWRvd24sKmtleXByZXNzLCprZXl1cCwqbG9hZCwqbG9hZGVkZGF0YSwqbG9hZGVkbWV0YWRhdGEsKmxvYWRzdGFydCwqbG9zdHBvaW50ZXJjYXB0dXJlLCptb3VzZWRvd24sKm1vdXNlZW50ZXIsKm1vdXNlbGVhdmUsKm1vdXNlbW92ZSwqbW91c2VvdXQsKm1vdXNlb3ZlciwqbW91c2V1cCwqbW91c2V3aGVlbCwqcGF1c2UsKnBsYXksKnBsYXlpbmcsKnBvaW50ZXJjYW5jZWwsKnBvaW50ZXJkb3duLCpwb2ludGVyZW50ZXIsKnBvaW50ZXJsZWF2ZSwqcG9pbnRlcm1vdmUsKnBvaW50ZXJvdXQsKnBvaW50ZXJvdmVyLCpwb2ludGVydXAsKnByb2dyZXNzLCpyYXRlY2hhbmdlLCpyZXNldCwqcmVzaXplLCpzY3JvbGwsKnNlZWtlZCwqc2Vla2luZywqc2VsZWN0LCpzaG93LCpzdGFsbGVkLCpzdWJtaXQsKnN1c3BlbmQsKnRpbWV1cGRhdGUsKnRvZ2dsZSwqdm9sdW1lY2hhbmdlLCp3YWl0aW5nLG91dGVyVGV4dCwhc3BlbGxjaGVjaywlc3R5bGUsI3RhYkluZGV4LHRpdGxlLCF0cmFuc2xhdGUnLFxuICAnbWVkaWFeW0hUTUxFbGVtZW50XXwhYXV0b3BsYXksIWNvbnRyb2xzLCVjb250cm9sc0xpc3QsJWNyb3NzT3JpZ2luLCNjdXJyZW50VGltZSwhZGVmYXVsdE11dGVkLCNkZWZhdWx0UGxheWJhY2tSYXRlLCFkaXNhYmxlUmVtb3RlUGxheWJhY2ssIWxvb3AsIW11dGVkLCplbmNyeXB0ZWQsKndhaXRpbmdmb3JrZXksI3BsYXliYWNrUmF0ZSxwcmVsb2FkLHNyYywlc3JjT2JqZWN0LCN2b2x1bWUnLFxuICAnOnN2ZzpeW0hUTUxFbGVtZW50XXwqYWJvcnQsKmF1eGNsaWNrLCpibHVyLCpjYW5jZWwsKmNhbnBsYXksKmNhbnBsYXl0aHJvdWdoLCpjaGFuZ2UsKmNsaWNrLCpjbG9zZSwqY29udGV4dG1lbnUsKmN1ZWNoYW5nZSwqZGJsY2xpY2ssKmRyYWcsKmRyYWdlbmQsKmRyYWdlbnRlciwqZHJhZ2xlYXZlLCpkcmFnb3ZlciwqZHJhZ3N0YXJ0LCpkcm9wLCpkdXJhdGlvbmNoYW5nZSwqZW1wdGllZCwqZW5kZWQsKmVycm9yLCpmb2N1cywqZ290cG9pbnRlcmNhcHR1cmUsKmlucHV0LCppbnZhbGlkLCprZXlkb3duLCprZXlwcmVzcywqa2V5dXAsKmxvYWQsKmxvYWRlZGRhdGEsKmxvYWRlZG1ldGFkYXRhLCpsb2Fkc3RhcnQsKmxvc3Rwb2ludGVyY2FwdHVyZSwqbW91c2Vkb3duLCptb3VzZWVudGVyLCptb3VzZWxlYXZlLCptb3VzZW1vdmUsKm1vdXNlb3V0LCptb3VzZW92ZXIsKm1vdXNldXAsKm1vdXNld2hlZWwsKnBhdXNlLCpwbGF5LCpwbGF5aW5nLCpwb2ludGVyY2FuY2VsLCpwb2ludGVyZG93biwqcG9pbnRlcmVudGVyLCpwb2ludGVybGVhdmUsKnBvaW50ZXJtb3ZlLCpwb2ludGVyb3V0LCpwb2ludGVyb3ZlciwqcG9pbnRlcnVwLCpwcm9ncmVzcywqcmF0ZWNoYW5nZSwqcmVzZXQsKnJlc2l6ZSwqc2Nyb2xsLCpzZWVrZWQsKnNlZWtpbmcsKnNlbGVjdCwqc2hvdywqc3RhbGxlZCwqc3VibWl0LCpzdXNwZW5kLCp0aW1ldXBkYXRlLCp0b2dnbGUsKnZvbHVtZWNoYW5nZSwqd2FpdGluZywlc3R5bGUsI3RhYkluZGV4JyxcbiAgJzpzdmc6Z3JhcGhpY3NeOnN2Zzp8JyxcbiAgJzpzdmc6YW5pbWF0aW9uXjpzdmc6fCpiZWdpbiwqZW5kLCpyZXBlYXQnLFxuICAnOnN2ZzpnZW9tZXRyeV46c3ZnOnwnLFxuICAnOnN2Zzpjb21wb25lbnRUcmFuc2ZlckZ1bmN0aW9uXjpzdmc6fCcsXG4gICc6c3ZnOmdyYWRpZW50Xjpzdmc6fCcsXG4gICc6c3ZnOnRleHRDb250ZW50Xjpzdmc6Z3JhcGhpY3N8JyxcbiAgJzpzdmc6dGV4dFBvc2l0aW9uaW5nXjpzdmc6dGV4dENvbnRlbnR8JyxcbiAgJ2FeW0hUTUxFbGVtZW50XXxjaGFyc2V0LGNvb3Jkcyxkb3dubG9hZCxoYXNoLGhvc3QsaG9zdG5hbWUsaHJlZixocmVmbGFuZyxuYW1lLHBhc3N3b3JkLHBhdGhuYW1lLHBpbmcscG9ydCxwcm90b2NvbCxyZWZlcnJlclBvbGljeSxyZWwscmV2LHNlYXJjaCxzaGFwZSx0YXJnZXQsdGV4dCx0eXBlLHVzZXJuYW1lJyxcbiAgJ2FyZWFeW0hUTUxFbGVtZW50XXxhbHQsY29vcmRzLGRvd25sb2FkLGhhc2gsaG9zdCxob3N0bmFtZSxocmVmLCFub0hyZWYscGFzc3dvcmQscGF0aG5hbWUscGluZyxwb3J0LHByb3RvY29sLHJlZmVycmVyUG9saWN5LHJlbCxzZWFyY2gsc2hhcGUsdGFyZ2V0LHVzZXJuYW1lJyxcbiAgJ2F1ZGlvXm1lZGlhfCcsXG4gICdicl5bSFRNTEVsZW1lbnRdfGNsZWFyJyxcbiAgJ2Jhc2VeW0hUTUxFbGVtZW50XXxocmVmLHRhcmdldCcsXG4gICdib2R5XltIVE1MRWxlbWVudF18YUxpbmssYmFja2dyb3VuZCxiZ0NvbG9yLGxpbmssKmJlZm9yZXVubG9hZCwqYmx1ciwqZXJyb3IsKmZvY3VzLCpoYXNoY2hhbmdlLCpsYW5ndWFnZWNoYW5nZSwqbG9hZCwqbWVzc2FnZSwqb2ZmbGluZSwqb25saW5lLCpwYWdlaGlkZSwqcGFnZXNob3csKnBvcHN0YXRlLCpyZWplY3Rpb25oYW5kbGVkLCpyZXNpemUsKnNjcm9sbCwqc3RvcmFnZSwqdW5oYW5kbGVkcmVqZWN0aW9uLCp1bmxvYWQsdGV4dCx2TGluaycsXG4gICdidXR0b25eW0hUTUxFbGVtZW50XXwhYXV0b2ZvY3VzLCFkaXNhYmxlZCxmb3JtQWN0aW9uLGZvcm1FbmN0eXBlLGZvcm1NZXRob2QsIWZvcm1Ob1ZhbGlkYXRlLGZvcm1UYXJnZXQsbmFtZSx0eXBlLHZhbHVlJyxcbiAgJ2NhbnZhc15bSFRNTEVsZW1lbnRdfCNoZWlnaHQsI3dpZHRoJyxcbiAgJ2NvbnRlbnReW0hUTUxFbGVtZW50XXxzZWxlY3QnLFxuICAnZGxeW0hUTUxFbGVtZW50XXwhY29tcGFjdCcsXG4gICdkYXRhbGlzdF5bSFRNTEVsZW1lbnRdfCcsXG4gICdkZXRhaWxzXltIVE1MRWxlbWVudF18IW9wZW4nLFxuICAnZGlhbG9nXltIVE1MRWxlbWVudF18IW9wZW4scmV0dXJuVmFsdWUnLFxuICAnZGlyXltIVE1MRWxlbWVudF18IWNvbXBhY3QnLFxuICAnZGl2XltIVE1MRWxlbWVudF18YWxpZ24nLFxuICAnZW1iZWReW0hUTUxFbGVtZW50XXxhbGlnbixoZWlnaHQsbmFtZSxzcmMsdHlwZSx3aWR0aCcsXG4gICdmaWVsZHNldF5bSFRNTEVsZW1lbnRdfCFkaXNhYmxlZCxuYW1lJyxcbiAgJ2ZvbnReW0hUTUxFbGVtZW50XXxjb2xvcixmYWNlLHNpemUnLFxuICAnZm9ybV5bSFRNTEVsZW1lbnRdfGFjY2VwdENoYXJzZXQsYWN0aW9uLGF1dG9jb21wbGV0ZSxlbmNvZGluZyxlbmN0eXBlLG1ldGhvZCxuYW1lLCFub1ZhbGlkYXRlLHRhcmdldCcsXG4gICdmcmFtZV5bSFRNTEVsZW1lbnRdfGZyYW1lQm9yZGVyLGxvbmdEZXNjLG1hcmdpbkhlaWdodCxtYXJnaW5XaWR0aCxuYW1lLCFub1Jlc2l6ZSxzY3JvbGxpbmcsc3JjJyxcbiAgJ2ZyYW1lc2V0XltIVE1MRWxlbWVudF18Y29scywqYmVmb3JldW5sb2FkLCpibHVyLCplcnJvciwqZm9jdXMsKmhhc2hjaGFuZ2UsKmxhbmd1YWdlY2hhbmdlLCpsb2FkLCptZXNzYWdlLCpvZmZsaW5lLCpvbmxpbmUsKnBhZ2VoaWRlLCpwYWdlc2hvdywqcG9wc3RhdGUsKnJlamVjdGlvbmhhbmRsZWQsKnJlc2l6ZSwqc2Nyb2xsLCpzdG9yYWdlLCp1bmhhbmRsZWRyZWplY3Rpb24sKnVubG9hZCxyb3dzJyxcbiAgJ2hyXltIVE1MRWxlbWVudF18YWxpZ24sY29sb3IsIW5vU2hhZGUsc2l6ZSx3aWR0aCcsXG4gICdoZWFkXltIVE1MRWxlbWVudF18JyxcbiAgJ2gxLGgyLGgzLGg0LGg1LGg2XltIVE1MRWxlbWVudF18YWxpZ24nLFxuICAnaHRtbF5bSFRNTEVsZW1lbnRdfHZlcnNpb24nLFxuICAnaWZyYW1lXltIVE1MRWxlbWVudF18YWxpZ24sIWFsbG93RnVsbHNjcmVlbixmcmFtZUJvcmRlcixoZWlnaHQsbG9uZ0Rlc2MsbWFyZ2luSGVpZ2h0LG1hcmdpbldpZHRoLG5hbWUscmVmZXJyZXJQb2xpY3ksJXNhbmRib3gsc2Nyb2xsaW5nLHNyYyxzcmNkb2Msd2lkdGgnLFxuICAnaW1nXltIVE1MRWxlbWVudF18YWxpZ24sYWx0LGJvcmRlciwlY3Jvc3NPcmlnaW4sI2hlaWdodCwjaHNwYWNlLCFpc01hcCxsb25nRGVzYyxsb3dzcmMsbmFtZSxyZWZlcnJlclBvbGljeSxzaXplcyxzcmMsc3Jjc2V0LHVzZU1hcCwjdnNwYWNlLCN3aWR0aCcsXG4gICdpbnB1dF5bSFRNTEVsZW1lbnRdfGFjY2VwdCxhbGlnbixhbHQsYXV0b2NhcGl0YWxpemUsYXV0b2NvbXBsZXRlLCFhdXRvZm9jdXMsIWNoZWNrZWQsIWRlZmF1bHRDaGVja2VkLGRlZmF1bHRWYWx1ZSxkaXJOYW1lLCFkaXNhYmxlZCwlZmlsZXMsZm9ybUFjdGlvbixmb3JtRW5jdHlwZSxmb3JtTWV0aG9kLCFmb3JtTm9WYWxpZGF0ZSxmb3JtVGFyZ2V0LCNoZWlnaHQsIWluY3JlbWVudGFsLCFpbmRldGVybWluYXRlLG1heCwjbWF4TGVuZ3RoLG1pbiwjbWluTGVuZ3RoLCFtdWx0aXBsZSxuYW1lLHBhdHRlcm4scGxhY2Vob2xkZXIsIXJlYWRPbmx5LCFyZXF1aXJlZCxzZWxlY3Rpb25EaXJlY3Rpb24sI3NlbGVjdGlvbkVuZCwjc2VsZWN0aW9uU3RhcnQsI3NpemUsc3JjLHN0ZXAsdHlwZSx1c2VNYXAsdmFsdWUsJXZhbHVlQXNEYXRlLCN2YWx1ZUFzTnVtYmVyLCN3aWR0aCcsXG4gICdsaV5bSFRNTEVsZW1lbnRdfHR5cGUsI3ZhbHVlJyxcbiAgJ2xhYmVsXltIVE1MRWxlbWVudF18aHRtbEZvcicsXG4gICdsZWdlbmReW0hUTUxFbGVtZW50XXxhbGlnbicsXG4gICdsaW5rXltIVE1MRWxlbWVudF18YXMsY2hhcnNldCwlY3Jvc3NPcmlnaW4sIWRpc2FibGVkLGhyZWYsaHJlZmxhbmcsaW50ZWdyaXR5LG1lZGlhLHJlZmVycmVyUG9saWN5LHJlbCwlcmVsTGlzdCxyZXYsJXNpemVzLHRhcmdldCx0eXBlJyxcbiAgJ21hcF5bSFRNTEVsZW1lbnRdfG5hbWUnLFxuICAnbWFycXVlZV5bSFRNTEVsZW1lbnRdfGJlaGF2aW9yLGJnQ29sb3IsZGlyZWN0aW9uLGhlaWdodCwjaHNwYWNlLCNsb29wLCNzY3JvbGxBbW91bnQsI3Njcm9sbERlbGF5LCF0cnVlU3BlZWQsI3ZzcGFjZSx3aWR0aCcsXG4gICdtZW51XltIVE1MRWxlbWVudF18IWNvbXBhY3QnLFxuICAnbWV0YV5bSFRNTEVsZW1lbnRdfGNvbnRlbnQsaHR0cEVxdWl2LG5hbWUsc2NoZW1lJyxcbiAgJ21ldGVyXltIVE1MRWxlbWVudF18I2hpZ2gsI2xvdywjbWF4LCNtaW4sI29wdGltdW0sI3ZhbHVlJyxcbiAgJ2lucyxkZWxeW0hUTUxFbGVtZW50XXxjaXRlLGRhdGVUaW1lJyxcbiAgJ29sXltIVE1MRWxlbWVudF18IWNvbXBhY3QsIXJldmVyc2VkLCNzdGFydCx0eXBlJyxcbiAgJ29iamVjdF5bSFRNTEVsZW1lbnRdfGFsaWduLGFyY2hpdmUsYm9yZGVyLGNvZGUsY29kZUJhc2UsY29kZVR5cGUsZGF0YSwhZGVjbGFyZSxoZWlnaHQsI2hzcGFjZSxuYW1lLHN0YW5kYnksdHlwZSx1c2VNYXAsI3ZzcGFjZSx3aWR0aCcsXG4gICdvcHRncm91cF5bSFRNTEVsZW1lbnRdfCFkaXNhYmxlZCxsYWJlbCcsXG4gICdvcHRpb25eW0hUTUxFbGVtZW50XXwhZGVmYXVsdFNlbGVjdGVkLCFkaXNhYmxlZCxsYWJlbCwhc2VsZWN0ZWQsdGV4dCx2YWx1ZScsXG4gICdvdXRwdXReW0hUTUxFbGVtZW50XXxkZWZhdWx0VmFsdWUsJWh0bWxGb3IsbmFtZSx2YWx1ZScsXG4gICdwXltIVE1MRWxlbWVudF18YWxpZ24nLFxuICAncGFyYW1eW0hUTUxFbGVtZW50XXxuYW1lLHR5cGUsdmFsdWUsdmFsdWVUeXBlJyxcbiAgJ3BpY3R1cmVeW0hUTUxFbGVtZW50XXwnLFxuICAncHJlXltIVE1MRWxlbWVudF18I3dpZHRoJyxcbiAgJ3Byb2dyZXNzXltIVE1MRWxlbWVudF18I21heCwjdmFsdWUnLFxuICAncSxibG9ja3F1b3RlLGNpdGVeW0hUTUxFbGVtZW50XXwnLFxuICAnc2NyaXB0XltIVE1MRWxlbWVudF18IWFzeW5jLGNoYXJzZXQsJWNyb3NzT3JpZ2luLCFkZWZlcixldmVudCxodG1sRm9yLGludGVncml0eSxzcmMsdGV4dCx0eXBlJyxcbiAgJ3NlbGVjdF5bSFRNTEVsZW1lbnRdfCFhdXRvZm9jdXMsIWRpc2FibGVkLCNsZW5ndGgsIW11bHRpcGxlLG5hbWUsIXJlcXVpcmVkLCNzZWxlY3RlZEluZGV4LCNzaXplLHZhbHVlJyxcbiAgJ3NoYWRvd15bSFRNTEVsZW1lbnRdfCcsXG4gICdzbG90XltIVE1MRWxlbWVudF18bmFtZScsXG4gICdzb3VyY2VeW0hUTUxFbGVtZW50XXxtZWRpYSxzaXplcyxzcmMsc3Jjc2V0LHR5cGUnLFxuICAnc3Bhbl5bSFRNTEVsZW1lbnRdfCcsXG4gICdzdHlsZV5bSFRNTEVsZW1lbnRdfCFkaXNhYmxlZCxtZWRpYSx0eXBlJyxcbiAgJ2NhcHRpb25eW0hUTUxFbGVtZW50XXxhbGlnbicsXG4gICd0aCx0ZF5bSFRNTEVsZW1lbnRdfGFiYnIsYWxpZ24sYXhpcyxiZ0NvbG9yLGNoLGNoT2ZmLCNjb2xTcGFuLGhlYWRlcnMsaGVpZ2h0LCFub1dyYXAsI3Jvd1NwYW4sc2NvcGUsdkFsaWduLHdpZHRoJyxcbiAgJ2NvbCxjb2xncm91cF5bSFRNTEVsZW1lbnRdfGFsaWduLGNoLGNoT2ZmLCNzcGFuLHZBbGlnbix3aWR0aCcsXG4gICd0YWJsZV5bSFRNTEVsZW1lbnRdfGFsaWduLGJnQ29sb3IsYm9yZGVyLCVjYXB0aW9uLGNlbGxQYWRkaW5nLGNlbGxTcGFjaW5nLGZyYW1lLHJ1bGVzLHN1bW1hcnksJXRGb290LCV0SGVhZCx3aWR0aCcsXG4gICd0cl5bSFRNTEVsZW1lbnRdfGFsaWduLGJnQ29sb3IsY2gsY2hPZmYsdkFsaWduJyxcbiAgJ3Rmb290LHRoZWFkLHRib2R5XltIVE1MRWxlbWVudF18YWxpZ24sY2gsY2hPZmYsdkFsaWduJyxcbiAgJ3RlbXBsYXRlXltIVE1MRWxlbWVudF18JyxcbiAgJ3RleHRhcmVhXltIVE1MRWxlbWVudF18YXV0b2NhcGl0YWxpemUsIWF1dG9mb2N1cywjY29scyxkZWZhdWx0VmFsdWUsZGlyTmFtZSwhZGlzYWJsZWQsI21heExlbmd0aCwjbWluTGVuZ3RoLG5hbWUscGxhY2Vob2xkZXIsIXJlYWRPbmx5LCFyZXF1aXJlZCwjcm93cyxzZWxlY3Rpb25EaXJlY3Rpb24sI3NlbGVjdGlvbkVuZCwjc2VsZWN0aW9uU3RhcnQsdmFsdWUsd3JhcCcsXG4gICd0aXRsZV5bSFRNTEVsZW1lbnRdfHRleHQnLFxuICAndHJhY2teW0hUTUxFbGVtZW50XXwhZGVmYXVsdCxraW5kLGxhYmVsLHNyYyxzcmNsYW5nJyxcbiAgJ3VsXltIVE1MRWxlbWVudF18IWNvbXBhY3QsdHlwZScsXG4gICd1bmtub3duXltIVE1MRWxlbWVudF18JyxcbiAgJ3ZpZGVvXm1lZGlhfCNoZWlnaHQscG9zdGVyLCN3aWR0aCcsXG4gICc6c3ZnOmFeOnN2ZzpncmFwaGljc3wnLFxuICAnOnN2ZzphbmltYXRlXjpzdmc6YW5pbWF0aW9ufCcsXG4gICc6c3ZnOmFuaW1hdGVNb3Rpb25eOnN2ZzphbmltYXRpb258JyxcbiAgJzpzdmc6YW5pbWF0ZVRyYW5zZm9ybV46c3ZnOmFuaW1hdGlvbnwnLFxuICAnOnN2ZzpjaXJjbGVeOnN2ZzpnZW9tZXRyeXwnLFxuICAnOnN2ZzpjbGlwUGF0aF46c3ZnOmdyYXBoaWNzfCcsXG4gICc6c3ZnOmRlZnNeOnN2ZzpncmFwaGljc3wnLFxuICAnOnN2ZzpkZXNjXjpzdmc6fCcsXG4gICc6c3ZnOmRpc2NhcmReOnN2Zzp8JyxcbiAgJzpzdmc6ZWxsaXBzZV46c3ZnOmdlb21ldHJ5fCcsXG4gICc6c3ZnOmZlQmxlbmReOnN2Zzp8JyxcbiAgJzpzdmc6ZmVDb2xvck1hdHJpeF46c3ZnOnwnLFxuICAnOnN2ZzpmZUNvbXBvbmVudFRyYW5zZmVyXjpzdmc6fCcsXG4gICc6c3ZnOmZlQ29tcG9zaXRlXjpzdmc6fCcsXG4gICc6c3ZnOmZlQ29udm9sdmVNYXRyaXheOnN2Zzp8JyxcbiAgJzpzdmc6ZmVEaWZmdXNlTGlnaHRpbmdeOnN2Zzp8JyxcbiAgJzpzdmc6ZmVEaXNwbGFjZW1lbnRNYXBeOnN2Zzp8JyxcbiAgJzpzdmc6ZmVEaXN0YW50TGlnaHReOnN2Zzp8JyxcbiAgJzpzdmc6ZmVEcm9wU2hhZG93Xjpzdmc6fCcsXG4gICc6c3ZnOmZlRmxvb2ReOnN2Zzp8JyxcbiAgJzpzdmc6ZmVGdW5jQV46c3ZnOmNvbXBvbmVudFRyYW5zZmVyRnVuY3Rpb258JyxcbiAgJzpzdmc6ZmVGdW5jQl46c3ZnOmNvbXBvbmVudFRyYW5zZmVyRnVuY3Rpb258JyxcbiAgJzpzdmc6ZmVGdW5jR146c3ZnOmNvbXBvbmVudFRyYW5zZmVyRnVuY3Rpb258JyxcbiAgJzpzdmc6ZmVGdW5jUl46c3ZnOmNvbXBvbmVudFRyYW5zZmVyRnVuY3Rpb258JyxcbiAgJzpzdmc6ZmVHYXVzc2lhbkJsdXJeOnN2Zzp8JyxcbiAgJzpzdmc6ZmVJbWFnZV46c3ZnOnwnLFxuICAnOnN2ZzpmZU1lcmdlXjpzdmc6fCcsXG4gICc6c3ZnOmZlTWVyZ2VOb2RlXjpzdmc6fCcsXG4gICc6c3ZnOmZlTW9ycGhvbG9neV46c3ZnOnwnLFxuICAnOnN2ZzpmZU9mZnNldF46c3ZnOnwnLFxuICAnOnN2ZzpmZVBvaW50TGlnaHReOnN2Zzp8JyxcbiAgJzpzdmc6ZmVTcGVjdWxhckxpZ2h0aW5nXjpzdmc6fCcsXG4gICc6c3ZnOmZlU3BvdExpZ2h0Xjpzdmc6fCcsXG4gICc6c3ZnOmZlVGlsZV46c3ZnOnwnLFxuICAnOnN2ZzpmZVR1cmJ1bGVuY2VeOnN2Zzp8JyxcbiAgJzpzdmc6ZmlsdGVyXjpzdmc6fCcsXG4gICc6c3ZnOmZvcmVpZ25PYmplY3ReOnN2ZzpncmFwaGljc3wnLFxuICAnOnN2ZzpnXjpzdmc6Z3JhcGhpY3N8JyxcbiAgJzpzdmc6aW1hZ2VeOnN2ZzpncmFwaGljc3wnLFxuICAnOnN2ZzpsaW5lXjpzdmc6Z2VvbWV0cnl8JyxcbiAgJzpzdmc6bGluZWFyR3JhZGllbnReOnN2ZzpncmFkaWVudHwnLFxuICAnOnN2ZzptcGF0aF46c3ZnOnwnLFxuICAnOnN2ZzptYXJrZXJeOnN2Zzp8JyxcbiAgJzpzdmc6bWFza146c3ZnOnwnLFxuICAnOnN2ZzptZXRhZGF0YV46c3ZnOnwnLFxuICAnOnN2ZzpwYXRoXjpzdmc6Z2VvbWV0cnl8JyxcbiAgJzpzdmc6cGF0dGVybl46c3ZnOnwnLFxuICAnOnN2Zzpwb2x5Z29uXjpzdmc6Z2VvbWV0cnl8JyxcbiAgJzpzdmc6cG9seWxpbmVeOnN2ZzpnZW9tZXRyeXwnLFxuICAnOnN2ZzpyYWRpYWxHcmFkaWVudF46c3ZnOmdyYWRpZW50fCcsXG4gICc6c3ZnOnJlY3ReOnN2ZzpnZW9tZXRyeXwnLFxuICAnOnN2ZzpzdmdeOnN2ZzpncmFwaGljc3wjY3VycmVudFNjYWxlLCN6b29tQW5kUGFuJyxcbiAgJzpzdmc6c2NyaXB0Xjpzdmc6fHR5cGUnLFxuICAnOnN2ZzpzZXReOnN2ZzphbmltYXRpb258JyxcbiAgJzpzdmc6c3RvcF46c3ZnOnwnLFxuICAnOnN2ZzpzdHlsZV46c3ZnOnwhZGlzYWJsZWQsbWVkaWEsdGl0bGUsdHlwZScsXG4gICc6c3ZnOnN3aXRjaF46c3ZnOmdyYXBoaWNzfCcsXG4gICc6c3ZnOnN5bWJvbF46c3ZnOnwnLFxuICAnOnN2Zzp0c3Bhbl46c3ZnOnRleHRQb3NpdGlvbmluZ3wnLFxuICAnOnN2Zzp0ZXh0Xjpzdmc6dGV4dFBvc2l0aW9uaW5nfCcsXG4gICc6c3ZnOnRleHRQYXRoXjpzdmc6dGV4dENvbnRlbnR8JyxcbiAgJzpzdmc6dGl0bGVeOnN2Zzp8JyxcbiAgJzpzdmc6dXNlXjpzdmc6Z3JhcGhpY3N8JyxcbiAgJzpzdmc6dmlld146c3ZnOnwjem9vbUFuZFBhbicsXG4gICdkYXRhXltIVE1MRWxlbWVudF18dmFsdWUnLFxuICAna2V5Z2VuXltIVE1MRWxlbWVudF18IWF1dG9mb2N1cyxjaGFsbGVuZ2UsIWRpc2FibGVkLGZvcm0sa2V5dHlwZSxuYW1lJyxcbiAgJ21lbnVpdGVtXltIVE1MRWxlbWVudF18dHlwZSxsYWJlbCxpY29uLCFkaXNhYmxlZCwhY2hlY2tlZCxyYWRpb2dyb3VwLCFkZWZhdWx0JyxcbiAgJ3N1bW1hcnleW0hUTUxFbGVtZW50XXwnLFxuICAndGltZV5bSFRNTEVsZW1lbnRdfGRhdGVUaW1lJyxcbiAgJzpzdmc6Y3Vyc29yXjpzdmc6fCcsXG5dO1xuXG5jb25zdCBhdHRyVG9Qcm9wTWFwOiB7W25hbWU6IHN0cmluZ106IHN0cmluZ30gPSA8YW55PntcbiAgJ2NsYXNzJzogJ2NsYXNzTmFtZScsXG4gICdmb3JtYWN0aW9uJzogJ2Zvcm1BY3Rpb24nLFxuICAnaW5uZXJIdG1sJzogJ2lubmVySFRNTCcsXG4gICdyZWFkb25seSc6ICdyZWFkT25seScsXG4gICd0YWJpbmRleCc6ICd0YWJJbmRleCdcbn07XG5cbmNvbnN0IEVWRU5UID0gJ2V2ZW50JztcbmNvbnN0IEJPT0xFQU4gPSAnYm9vbGVhbic7XG5jb25zdCBOVU1CRVIgPSAnbnVtYmVyJztcbmNvbnN0IFNUUklORyA9ICdzdHJpbmcnO1xuY29uc3QgT0JKRUNUID0gJ29iamVjdCc7XG5cbmV4cG9ydCBjbGFzcyBTY2hlbWFJbmZvcm1hdGlvbiB7XG4gIHNjaGVtYSA9IDx7W2VsZW1lbnQ6IHN0cmluZ106IHtbcHJvcGVydHk6IHN0cmluZ106IHN0cmluZ319Pnt9O1xuXG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIFNDSEVNQS5mb3JFYWNoKGVuY29kZWRUeXBlID0+IHtcbiAgICAgIGNvbnN0IHBhcnRzID0gZW5jb2RlZFR5cGUuc3BsaXQoJ3wnKTtcbiAgICAgIGNvbnN0IHByb3BlcnRpZXMgPSBwYXJ0c1sxXS5zcGxpdCgnLCcpO1xuICAgICAgY29uc3QgdHlwZVBhcnRzID0gKHBhcnRzWzBdICsgJ14nKS5zcGxpdCgnXicpO1xuICAgICAgY29uc3QgdHlwZU5hbWUgPSB0eXBlUGFydHNbMF07XG4gICAgICBjb25zdCB0eXBlID0gPHtbcHJvcGVydHk6IHN0cmluZ106IHN0cmluZ30+e307XG4gICAgICB0eXBlTmFtZS5zcGxpdCgnLCcpLmZvckVhY2godGFnID0+IHRoaXMuc2NoZW1hW3RhZy50b0xvd2VyQ2FzZSgpXSA9IHR5cGUpO1xuICAgICAgY29uc3Qgc3VwZXJOYW1lID0gdHlwZVBhcnRzWzFdO1xuICAgICAgY29uc3Qgc3VwZXJUeXBlID0gc3VwZXJOYW1lICYmIHRoaXMuc2NoZW1hW3N1cGVyTmFtZS50b0xvd2VyQ2FzZSgpXTtcbiAgICAgIGlmIChzdXBlclR5cGUpIHtcbiAgICAgICAgZm9yIChjb25zdCBrZXkgaW4gc3VwZXJUeXBlKSB7XG4gICAgICAgICAgdHlwZVtrZXldID0gc3VwZXJUeXBlW2tleV07XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIHByb3BlcnRpZXMuZm9yRWFjaCgocHJvcGVydHk6IHN0cmluZykgPT4ge1xuICAgICAgICBpZiAocHJvcGVydHkgPT0gJycpIHtcbiAgICAgICAgfSBlbHNlIGlmIChwcm9wZXJ0eS5zdGFydHNXaXRoKCcqJykpIHtcbiAgICAgICAgICB0eXBlW3Byb3BlcnR5LnN1YnN0cmluZygxKV0gPSBFVkVOVDtcbiAgICAgICAgfSBlbHNlIGlmIChwcm9wZXJ0eS5zdGFydHNXaXRoKCchJykpIHtcbiAgICAgICAgICB0eXBlW3Byb3BlcnR5LnN1YnN0cmluZygxKV0gPSBCT09MRUFOO1xuICAgICAgICB9IGVsc2UgaWYgKHByb3BlcnR5LnN0YXJ0c1dpdGgoJyMnKSkge1xuICAgICAgICAgIHR5cGVbcHJvcGVydHkuc3Vic3RyaW5nKDEpXSA9IE5VTUJFUjtcbiAgICAgICAgfSBlbHNlIGlmIChwcm9wZXJ0eS5zdGFydHNXaXRoKCclJykpIHtcbiAgICAgICAgICB0eXBlW3Byb3BlcnR5LnN1YnN0cmluZygxKV0gPSBPQkpFQ1Q7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgdHlwZVtwcm9wZXJ0eV0gPSBTVFJJTkc7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH0pO1xuICB9XG5cbiAgYWxsS25vd25FbGVtZW50cygpOiBzdHJpbmdbXSB7IHJldHVybiBPYmplY3Qua2V5cyh0aGlzLnNjaGVtYSk7IH1cblxuICBldmVudHNPZihlbGVtZW50TmFtZTogc3RyaW5nKTogc3RyaW5nW10ge1xuICAgIGNvbnN0IGVsZW1lbnRUeXBlID0gdGhpcy5zY2hlbWFbZWxlbWVudE5hbWUudG9Mb3dlckNhc2UoKV0gfHwge307XG4gICAgcmV0dXJuIE9iamVjdC5rZXlzKGVsZW1lbnRUeXBlKS5maWx0ZXIocHJvcGVydHkgPT4gZWxlbWVudFR5cGVbcHJvcGVydHldID09PSBFVkVOVCk7XG4gIH1cblxuICBwcm9wZXJ0aWVzT2YoZWxlbWVudE5hbWU6IHN0cmluZyk6IHN0cmluZ1tdIHtcbiAgICBjb25zdCBlbGVtZW50VHlwZSA9IHRoaXMuc2NoZW1hW2VsZW1lbnROYW1lLnRvTG93ZXJDYXNlKCldIHx8IHt9O1xuICAgIHJldHVybiBPYmplY3Qua2V5cyhlbGVtZW50VHlwZSkuZmlsdGVyKHByb3BlcnR5ID0+IGVsZW1lbnRUeXBlW3Byb3BlcnR5XSAhPT0gRVZFTlQpO1xuICB9XG5cbiAgdHlwZU9mKGVsZW1lbnROYW1lOiBzdHJpbmcsIHByb3BlcnR5OiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIHJldHVybiAodGhpcy5zY2hlbWFbZWxlbWVudE5hbWUudG9Mb3dlckNhc2UoKV0gfHwge30pW3Byb3BlcnR5XTtcbiAgfVxuXG4gIHByaXZhdGUgc3RhdGljIF9pbnN0YW5jZTogU2NoZW1hSW5mb3JtYXRpb247XG5cbiAgc3RhdGljIGdldCBpbnN0YW5jZSgpOiBTY2hlbWFJbmZvcm1hdGlvbiB7XG4gICAgbGV0IHJlc3VsdCA9IFNjaGVtYUluZm9ybWF0aW9uLl9pbnN0YW5jZTtcbiAgICBpZiAoIXJlc3VsdCkge1xuICAgICAgcmVzdWx0ID0gU2NoZW1hSW5mb3JtYXRpb24uX2luc3RhbmNlID0gbmV3IFNjaGVtYUluZm9ybWF0aW9uKCk7XG4gICAgfVxuICAgIHJldHVybiByZXN1bHQ7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGV2ZW50TmFtZXMoZWxlbWVudE5hbWU6IHN0cmluZyk6IHN0cmluZ1tdIHtcbiAgcmV0dXJuIFNjaGVtYUluZm9ybWF0aW9uLmluc3RhbmNlLmV2ZW50c09mKGVsZW1lbnROYW1lKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHByb3BlcnR5TmFtZXMoZWxlbWVudE5hbWU6IHN0cmluZyk6IHN0cmluZ1tdIHtcbiAgcmV0dXJuIFNjaGVtYUluZm9ybWF0aW9uLmluc3RhbmNlLnByb3BlcnRpZXNPZihlbGVtZW50TmFtZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBwcm9wZXJ0eVR5cGUoZWxlbWVudE5hbWU6IHN0cmluZywgcHJvcGVydHlOYW1lOiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gU2NoZW1hSW5mb3JtYXRpb24uaW5zdGFuY2UudHlwZU9mKGVsZW1lbnROYW1lLCBwcm9wZXJ0eU5hbWUpO1xufVxuIl19