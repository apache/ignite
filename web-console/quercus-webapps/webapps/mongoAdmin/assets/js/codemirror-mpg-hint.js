// CodeMirror, copyright (c) by Marijn Haverbeke and others
// Distributed under an MIT license: https://codemirror.net/LICENSE

(function(mod) {
  if (typeof exports == "object" && typeof module == "object") // CommonJS
    mod(require("../../lib/codemirror"));
  else if (typeof define == "function" && define.amd) // AMD
    define(["../../lib/codemirror"], mod);
  else // Plain browser env
    mod(CodeMirror);
})(function(CodeMirror) {
  "use strict";

  var WORD = /[\w$]+/, RANGE = 500;

  CodeMirror.registerHelper("hint", "anyword", function(editor, options) {

    var word = options && options.word || WORD;
    var range = options && options.range || RANGE;
    var cur = editor.getCursor(), curLine = editor.getLine(cur.line);
    var end = cur.ch, start = end;
    while (start && word.test(curLine.charAt(start - 1))) --start;
    var curWord = start != end && curLine.slice(start, end);

    options.list = MPG.mongoDBAndSQLKeywords.concat(MPG.collectionFields);

    /**
     * Code block taken from Mongolo project.
     * @see https://github.com/tetreum/mongolo/blob/develop/htdocs/js/src/custom-hint.js
     */

    var list = [],
        suggestedWord,
        k;

    if (curWord)
    {
      for (k in options.list)
      {
        if (!options.list.hasOwnProperty(k)) {continue;}
        suggestedWord = options.list[k];

        if (suggestedWord.toLowerCase().startsWith(curWord.toLowerCase()) && list.indexOf(suggestedWord) == -1) {
          list.push(suggestedWord);
        }
      }
    }

    return {list: list, from: CodeMirror.Pos(cur.line, start), to: CodeMirror.Pos(cur.line, end)};
  });
});
