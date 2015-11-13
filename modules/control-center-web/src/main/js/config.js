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

  map: {
    "ace": "github:ajaxorg/ace-builds@1.2.2",
    "angular": "github:angular/bower-angular@1.4.7",
    "angular-animate": "github:angular/bower-angular-animate@1.4.7",
    "angular-drag-and-drop-lists": "github:marceljuenemann/angular-drag-and-drop-lists@1.3.0",
    "angular-grid": "github:ceolter/ag-grid@2.3.5",
    "angular-loading": "github:darthwade/angular-loading@0.1.4",
    "angular-nvd3": "github:krispo/angular-nvd3@1.0.3",
    "angular-sanitize": "github:angular/bower-angular-sanitize@1.4.7",
    "angular-smart-table": "github:lorenzofox3/Smart-Table@2.1.4",
    "angular-strap": "github:akuznetsov-gridgain/angular-strap@fix-1852",
    "angular-tree-control": "github:wix/angular-tree-control@0.2.22",
    "angular-ui-ace": "github:angular-ui/ui-ace@0.2.3",
    "babel": "npm:babel-core@5.8.34",
    "babel-runtime": "npm:babel-runtime@5.8.34",
    "core-js": "npm:core-js@1.2.6",
    "jquery": "github:components/jquery@2.1.4",
    "lodash": "npm:lodash@3.10.1",
    "spinjs": "github:fgnass/spin.js@2.3.2",
    "github:akuznetsov-gridgain/angular-strap@fix-1852": {
      "angular": "github:angular/bower-angular@1.4.7"
    },
    "github:angular/bower-angular-animate@1.4.7": {
      "angular": "github:angular/bower-angular@1.4.7"
    },
    "github:angular/bower-angular-sanitize@1.4.7": {
      "angular": "github:angular/bower-angular@1.4.7"
    },
    "github:jspm/nodelibs-assert@0.1.0": {
      "assert": "npm:assert@1.3.0"
    },
    "github:jspm/nodelibs-path@0.1.0": {
      "path-browserify": "npm:path-browserify@0.0.0"
    },
    "github:jspm/nodelibs-process@0.1.2": {
      "process": "npm:process@0.11.2"
    },
    "github:jspm/nodelibs-util@0.1.0": {
      "util": "npm:util@0.10.3"
    },
    "github:krispo/angular-nvd3@1.0.3": {
      "angular": "github:angular/bower-angular@1.4.7",
      "d3": "npm:d3@3.5.8",
      "nvd3": "npm:nvd3@1.8.1"
    },
    "github:lorenzofox3/Smart-Table@2.1.4": {
      "angular": "github:angular/bower-angular@1.4.7"
    },
    "github:marceljuenemann/angular-drag-and-drop-lists@1.3.0": {
      "angular": "github:angular/bower-angular@1.4.7"
    },
    "github:wix/angular-tree-control@0.2.22": {
      "angular": "github:angular/bower-angular@1.4.7"
    },
    "npm:assert@1.3.0": {
      "util": "npm:util@0.10.3"
    },
    "npm:babel-runtime@5.8.34": {
      "process": "github:jspm/nodelibs-process@0.1.2"
    },
    "npm:core-js@1.2.6": {
      "fs": "github:jspm/nodelibs-fs@0.1.2",
      "path": "github:jspm/nodelibs-path@0.1.0",
      "process": "github:jspm/nodelibs-process@0.1.2",
      "systemjs-json": "github:systemjs/plugin-json@0.1.0"
    },
    "npm:inherits@2.0.1": {
      "util": "github:jspm/nodelibs-util@0.1.0"
    },
    "npm:lodash@3.10.1": {
      "process": "github:jspm/nodelibs-process@0.1.2"
    },
    "npm:nvd3@1.8.1": {
      "d3": "npm:d3@3.5.8"
    },
    "npm:path-browserify@0.0.0": {
      "process": "github:jspm/nodelibs-process@0.1.2"
    },
    "npm:process@0.11.2": {
      "assert": "github:jspm/nodelibs-assert@0.1.0"
    },
    "npm:util@0.10.3": {
      "inherits": "npm:inherits@2.0.1",
      "process": "github:jspm/nodelibs-process@0.1.2"
    }
  }
});
