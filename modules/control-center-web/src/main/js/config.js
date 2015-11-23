System.config({
  defaultJSExtensions: true,
  transpiler: "babel",
  babelOptions: {
    "optional": [
      "runtime",
      "optimisation.modules.system"
    ]
  },
  paths: {
    "github:*": "jspm_packages/github/*",
    "npm:*": "jspm_packages/npm/*"
  },
  separateCSS: true,
  map: {
    "Blob": "github:eligrey/Blob.js@master",
    "FileSaver": "github:eligrey/FileSaver.js@master",
    "ace": "github:ajaxorg/ace-builds@1.2.2",
    "angular": "github:angular/bower-angular@1.4.8",
    "angular-ag-grid": "github:ceolter/ag-grid@2.3.5",
    "angular-animate": "github:angular/bower-angular-animate@1.4.8",
    "angular-drag-and-drop-lists": "github:marceljuenemann/angular-drag-and-drop-lists@1.3.0",
    "angular-grid": "github:ceolter/ag-grid@2.3.5",
    "angular-loading": "github:darthwade/angular-loading@0.1.4",
    "angular-motion": "github:mgcrea/angular-motion@0.4.3",
    "angular-nvd3": "github:krispo/angular-nvd3@1.0.3",
    "angular-sanitize": "github:angular/bower-angular-sanitize@1.4.8",
    "angular-smart-table": "github:lorenzofox3/Smart-Table@2.1.5",
    "angular-strap": "github:akuznetsov-gridgain/angular-strap@fix-1852-2.3.6",
    "angular-tree-control": "github:wix/angular-tree-control@0.2.22",
    "angular-ui-ace": "github:angular-ui/ui-ace@0.2.3",
    "angular-ui-router": "github:angular-ui/ui-router@0.2.15",
    "babel": "npm:babel-core@5.8.34",
    "babel-runtime": "npm:babel-runtime@5.8.34",
    "blob": "github:eligrey/Blob.js@master",
    "bootstrap": "github:twbs/bootstrap@3.3.5",
    "bootstrap-carousel": "github:twbs/bootstrap@3.3.5",
    "clean-css": "npm:clean-css@3.4.8",
    "core-js": "npm:core-js@1.2.6",
    "css": "github:systemjs/plugin-css@0.1.19",
    "file-saver": "github:eligrey/FileSaver.js@master",
    "font-awesome": "npm:font-awesome@4.4.0",
    "jquery": "github:components/jquery@2.1.4",
    "jszip": "github:Stuk/jszip@2.5.0",
    "lodash": "npm:lodash@3.10.1",
    "nvd3": "npm:nvd3@1.8.1",
    "query-command-supported": "github:zenorocha/document.queryCommandSupported@1.0.0",
    "spinjs": "github:fgnass/spin.js@2.3.2",
    "github:akuznetsov-gridgain/angular-strap@fix-1852-2.3.6": {
      "angular-animate": "github:angular/bower-angular-animate@1.4.8",
      "angular-motion": "github:mgcrea/angular-motion@0.4.3",
      "angular-sanitize": "github:angular/bower-angular-sanitize@1.4.8"
    },
    "github:angular-ui/ui-ace@0.2.3": {
      "ace": "github:ajaxorg/ace-builds@1.2.2"
    },
    "github:angular-ui/ui-router@0.2.15": {
      "angular": "github:angular/bower-angular@1.4.8"
    },
    "github:angular/bower-angular-animate@1.4.8": {
      "angular": "github:angular/bower-angular@1.4.8"
    },
    "github:angular/bower-angular-sanitize@1.4.8": {
      "angular": "github:angular/bower-angular@1.4.8"
    },
    "github:angular/bower-angular@1.4.8": {
      "jquery": "github:components/jquery@2.1.4"
    },
    "github:darthwade/angular-loading@0.1.4": {
      "spinjs": "github:fgnass/spin.js@2.3.2"
    },
    "github:eligrey/FileSaver.js@master": {
      "blob": "github:eligrey/Blob.js@master"
    },
    "github:jspm/nodelibs-assert@0.1.0": {
      "assert": "npm:assert@1.3.0"
    },
    "github:jspm/nodelibs-buffer@0.1.0": {
      "buffer": "npm:buffer@3.5.2"
    },
    "github:jspm/nodelibs-events@0.1.1": {
      "events": "npm:events@1.0.2"
    },
    "github:jspm/nodelibs-http@1.7.1": {
      "Base64": "npm:Base64@0.2.1",
      "events": "github:jspm/nodelibs-events@0.1.1",
      "inherits": "npm:inherits@2.0.1",
      "stream": "github:jspm/nodelibs-stream@0.1.0",
      "url": "github:jspm/nodelibs-url@0.1.0",
      "util": "github:jspm/nodelibs-util@0.1.0"
    },
    "github:jspm/nodelibs-https@0.1.0": {
      "https-browserify": "npm:https-browserify@0.0.0"
    },
    "github:jspm/nodelibs-os@0.1.0": {
      "os-browserify": "npm:os-browserify@0.1.2"
    },
    "github:jspm/nodelibs-path@0.1.0": {
      "path-browserify": "npm:path-browserify@0.0.0"
    },
    "github:jspm/nodelibs-process@0.1.2": {
      "process": "npm:process@0.11.2"
    },
    "github:jspm/nodelibs-stream@0.1.0": {
      "stream-browserify": "npm:stream-browserify@1.0.0"
    },
    "github:jspm/nodelibs-url@0.1.0": {
      "url": "npm:url@0.10.3"
    },
    "github:jspm/nodelibs-util@0.1.0": {
      "util": "npm:util@0.10.3"
    },
    "github:krispo/angular-nvd3@1.0.3": {
      "angular": "github:angular/bower-angular@1.4.8",
      "d3": "npm:d3@3.5.9",
      "nvd3": "npm:nvd3@1.8.1"
    },
    "github:marceljuenemann/angular-drag-and-drop-lists@1.3.0": {
      "angular": "github:angular/bower-angular@1.4.8"
    },
    "github:mgcrea/angular-motion@0.4.3": {
      "angular": "github:angular/bower-angular@1.4.8",
      "css": "github:systemjs/plugin-css@0.1.19"
    },
    "github:twbs/bootstrap@3.3.5": {
      "jquery": "github:components/jquery@2.1.4"
    },
    "github:wix/angular-tree-control@0.2.22": {
      "angular": "github:angular/bower-angular@1.4.8"
    },
    "npm:amdefine@1.0.0": {
      "fs": "github:jspm/nodelibs-fs@0.1.2",
      "module": "github:jspm/nodelibs-module@0.1.0",
      "path": "github:jspm/nodelibs-path@0.1.0",
      "process": "github:jspm/nodelibs-process@0.1.2"
    },
    "npm:assert@1.3.0": {
      "util": "npm:util@0.10.3"
    },
    "npm:babel-runtime@5.8.34": {
      "process": "github:jspm/nodelibs-process@0.1.2"
    },
    "npm:buffer@3.5.2": {
      "base64-js": "npm:base64-js@0.0.8",
      "ieee754": "npm:ieee754@1.1.6",
      "is-array": "npm:is-array@1.0.1"
    },
    "npm:clean-css@3.4.8": {
      "buffer": "github:jspm/nodelibs-buffer@0.1.0",
      "commander": "npm:commander@2.8.1",
      "fs": "github:jspm/nodelibs-fs@0.1.2",
      "http": "github:jspm/nodelibs-http@1.7.1",
      "https": "github:jspm/nodelibs-https@0.1.0",
      "os": "github:jspm/nodelibs-os@0.1.0",
      "path": "github:jspm/nodelibs-path@0.1.0",
      "process": "github:jspm/nodelibs-process@0.1.2",
      "source-map": "npm:source-map@0.4.4",
      "url": "github:jspm/nodelibs-url@0.1.0",
      "util": "github:jspm/nodelibs-util@0.1.0"
    },
    "npm:commander@2.8.1": {
      "child_process": "github:jspm/nodelibs-child_process@0.1.0",
      "events": "github:jspm/nodelibs-events@0.1.1",
      "fs": "github:jspm/nodelibs-fs@0.1.2",
      "graceful-readlink": "npm:graceful-readlink@1.0.1",
      "path": "github:jspm/nodelibs-path@0.1.0",
      "process": "github:jspm/nodelibs-process@0.1.2"
    },
    "npm:core-js@1.2.6": {
      "fs": "github:jspm/nodelibs-fs@0.1.2",
      "path": "github:jspm/nodelibs-path@0.1.0",
      "process": "github:jspm/nodelibs-process@0.1.2",
      "systemjs-json": "github:systemjs/plugin-json@0.1.0"
    },
    "npm:core-util-is@1.0.2": {
      "buffer": "github:jspm/nodelibs-buffer@0.1.0"
    },
    "npm:font-awesome@4.4.0": {
      "css": "github:systemjs/plugin-css@0.1.19"
    },
    "npm:graceful-readlink@1.0.1": {
      "fs": "github:jspm/nodelibs-fs@0.1.2"
    },
    "npm:https-browserify@0.0.0": {
      "http": "github:jspm/nodelibs-http@1.7.1"
    },
    "npm:inherits@2.0.1": {
      "util": "github:jspm/nodelibs-util@0.1.0"
    },
    "npm:lodash@3.10.1": {
      "process": "github:jspm/nodelibs-process@0.1.2"
    },
    "npm:nvd3@1.8.1": {
      "d3": "npm:d3@3.5.9"
    },
    "npm:os-browserify@0.1.2": {
      "os": "github:jspm/nodelibs-os@0.1.0"
    },
    "npm:path-browserify@0.0.0": {
      "process": "github:jspm/nodelibs-process@0.1.2"
    },
    "npm:process@0.11.2": {
      "assert": "github:jspm/nodelibs-assert@0.1.0"
    },
    "npm:punycode@1.3.2": {
      "process": "github:jspm/nodelibs-process@0.1.2"
    },
    "npm:readable-stream@1.1.13": {
      "buffer": "github:jspm/nodelibs-buffer@0.1.0",
      "core-util-is": "npm:core-util-is@1.0.2",
      "events": "github:jspm/nodelibs-events@0.1.1",
      "inherits": "npm:inherits@2.0.1",
      "isarray": "npm:isarray@0.0.1",
      "process": "github:jspm/nodelibs-process@0.1.2",
      "stream-browserify": "npm:stream-browserify@1.0.0",
      "string_decoder": "npm:string_decoder@0.10.31"
    },
    "npm:source-map@0.4.4": {
      "amdefine": "npm:amdefine@1.0.0",
      "process": "github:jspm/nodelibs-process@0.1.2"
    },
    "npm:stream-browserify@1.0.0": {
      "events": "github:jspm/nodelibs-events@0.1.1",
      "inherits": "npm:inherits@2.0.1",
      "readable-stream": "npm:readable-stream@1.1.13"
    },
    "npm:string_decoder@0.10.31": {
      "buffer": "github:jspm/nodelibs-buffer@0.1.0"
    },
    "npm:url@0.10.3": {
      "assert": "github:jspm/nodelibs-assert@0.1.0",
      "punycode": "npm:punycode@1.3.2",
      "querystring": "npm:querystring@0.2.0",
      "util": "github:jspm/nodelibs-util@0.1.0"
    },
    "npm:util@0.10.3": {
      "inherits": "npm:inherits@2.0.1",
      "process": "github:jspm/nodelibs-process@0.1.2"
    }
  }
});
