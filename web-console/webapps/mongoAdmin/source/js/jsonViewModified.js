var JsonView = (function (exports) {
  'use strict';

  function _typeof(obj) {
    "@babel/helpers - typeof";

    if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") {
      _typeof = function (obj) {
        return typeof obj;
      };
    } else {
      _typeof = function (obj) {
        return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj;
      };
    }

    return _typeof(obj);
  }

  function expandedTemplate() {
    var params = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    var key = params.key,
        size = params.size;
    return "\n    <div class=\"line\">\n      <div class=\"caret-icon\"><i class=\"fas fa-caret-right\"></i></div>\n      <div class=\"json-key\">".concat(key, "</div>\n      <div class=\"json-size\">").concat(size, "</div>\n    </div>\n  ");
  }

  function notExpandedTemplate() {
    var params = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    var key = params.key,
        value = params.value,
        type = params.type,

        // XXX Modification made for MongoDB PHP GUI.
        documentFieldIsUpdatable = params.documentFieldIsUpdatable,
        documentId = params.documentId,
        documentFieldName = params.documentFieldName;

    // XXX Modification made for MongoDB PHP GUI.
    var title = ( documentFieldIsUpdatable === 'true' ) ? 'Edit value' : '';
    return "\n    <div class=\"line\">\n      <div class=\"empty-icon\"></div>\n      <div class=\"json-key\">".concat(key, "</div>\n      <div class=\"json-separator\">:</div>\n      <div data-document-id=\"" + documentId + "\" data-document-field-is-updatable=\"" + documentFieldIsUpdatable + "\" data-document-field-name=\"" + documentFieldName + "\" data-document-field-type=\"" + type + "\" title=\"" + title + "\" class=\"json-value json-").concat(type, "\">").concat(value, "</div>\n    </div>\n  ");
  }

  function hideNodeChildren(node) {
    node.children.forEach(function (child) {
      child.el.classList.add('hide');

      if (child.isExpanded) {
        hideNodeChildren(child);
      }
    });
  }

  function showNodeChildren(node) {
    node.children.forEach(function (child) {
      child.el.classList.remove('hide');

      if (child.isExpanded) {
        showNodeChildren(child);
      }
    });
  }

  function setCaretIconDown(node) {
    if (node.children.length > 0) {
      var icon = node.el.querySelector('.fas');

      if (icon) {
        icon.classList.replace('fa-caret-right', 'fa-caret-down');
      }
    }
  }

  function setCaretIconRight(node) {
    if (node.children.length > 0) {
      var icon = node.el.querySelector('.fas');

      if (icon) {
        icon.classList.replace('fa-caret-down', 'fa-caret-right');
      }
    }
  }

  function toggleNode(node) {
    if (node.isExpanded) {
      node.isExpanded = false;
      setCaretIconRight(node);
      hideNodeChildren(node);
    } else {
      node.isExpanded = true;
      setCaretIconDown(node);
      showNodeChildren(node);
    }
  }

  function createContainerElement() {
    var el = document.createElement('div');
    el.className = 'json-container';
    return el;
  }

  // XXX Modification made for MongoDB PHP GUI.
  function getDocFieldFromNode(node) {

    var docFieldArray = [];

    docFieldArray.push(node.key);

    switch (node.depth) {

      case 2:
        if ( node.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.key);
        }
        if ( node.parent.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.parent.key);
        }
        break;

      case 3:
        if ( node.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.key);
        }
        if ( node.parent.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.parent.key);
        }
        if ( node.parent.parent.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.parent.parent.key);
        }
        break;

      case 4:
        if ( node.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.key);
        }
        if ( node.parent.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.parent.key);
        }
        if ( node.parent.parent.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.parent.parent.key);
        }
        if ( node.parent.parent.parent.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.parent.parent.parent.key);
        }
        break;

      case 5:
        if ( node.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.key);
        }
        if ( node.parent.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.parent.key);
        }
        if ( node.parent.parent.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.parent.parent.key);
        }
        if ( node.parent.parent.parent.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.parent.parent.parent.key);
        }
        if ( node.parent.parent.parent.parent.parent.depth >= 2 ) {
          docFieldArray.push(node.parent.parent.parent.parent.parent.key);
        }
        break;
        
    }

    docFieldArray.reverse();

    return docFieldArray.join('.');

  }

  function createNodeElement(node) {
    var el = document.createElement('div');

    var getSizeString = function getSizeString(node) {
      var len = node.children.length;
      if (node.type === 'array') return "[".concat(len, "]");
      if (node.type === 'object') return "{".concat(len, "}");
      return null;
    };

    if (node.children.length > 0) {
      el.innerHTML = expandedTemplate({
        key: node.key,
        size: getSizeString(node)
      });
      var caretEl = el.querySelector('.caret-icon');
      caretEl.addEventListener('click', function () {
        toggleNode(node);
      });
    } else {

      // XXX Modification made for MongoDB PHP GUI.
      if ( node.key === '_id' && node.depth === 2 ) {
        MPG.documentId = node.value;
      }
      if ( node.depth >= 2 && node.depth <= 5 && node.key !== '_id' ) {
        var documentFieldIsUpdatable = true;
      } else {
        var documentFieldIsUpdatable = false;
      }
      if ( _typeof(node.value) === 'string' ) {
        node.value = MPG.helpers.escapeHTML(node.value);
      }

      el.innerHTML = notExpandedTemplate({
        key: node.key,
        value: node.value,
        type: _typeof(node.value),

        // XXX Modification made for MongoDB PHP GUI.
        documentFieldIsUpdatable: ( documentFieldIsUpdatable ) ? 'true' : 'false',
        documentId: MPG.documentId,
        documentFieldName: getDocFieldFromNode(node)

      });
    }

    var lineEl = el.children[0];

    if (node.parent !== null) {
      lineEl.classList.add('hide');
    }

    lineEl.style = 'margin-left: ' + node.depth * 18 + 'px;';
    return lineEl;
  }

  function getDataType(val) {
    var type = _typeof(val);

    if (Array.isArray(val)) type = 'array';
    if (val === null) type = 'null';
    return type;
  }

  function traverseTree(node, callback) {
    callback(node);

    if (node.children.length > 0) {
      node.children.forEach(function (child) {
        traverseTree(child, callback);
      });
    }
  }

  function createNode() {
    var opt = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    return {
      key: opt.key || null,
      parent: opt.parent || null,
      value: opt.hasOwnProperty('value') ? opt.value : null,
      isExpanded: opt.isExpanded || false,
      type: opt.type || null,
      children: opt.children || [],
      el: opt.el || null,
      depth: opt.depth || 0
    };
  }

  function createSubnode(data, node) {
    if (_typeof(data) === 'object') {
      for (var key in data) {
        var child = createNode({
          value: data[key],
          key: key,
          depth: node.depth + 1,
          type: getDataType(data[key]),
          parent: node
        });
        node.children.push(child);
        createSubnode(data[key], child);
      }
    }
  }

  function createTree(jsonData) {
    var data = typeof jsonData === 'string' ? JSON.parse(jsonData) : jsonData;
    var rootNode = createNode({
      value: data,
      key: getDataType(data),
      type: getDataType(data)
    });
    createSubnode(data, rootNode);
    return rootNode;
  }

  function renderJSON(jsonData, targetElement) {
    var parsedData = typeof jsonData === 'string' ? JSON.parse(jsonData) : jsonData;
    var tree = createTree(parsedData);
    render(tree, targetElement);
    return tree;
  }

  function render(tree, targetElement) {
    var containerEl = createContainerElement();
    traverseTree(tree, function (node) {
      node.el = createNodeElement(node);
      containerEl.appendChild(node.el);
    });
    targetElement.appendChild(containerEl);
  }

  function expandChildren(node) {
    traverseTree(node, function (child) {
      child.el.classList.remove('hide');
      child.isExpanded = true;
      setCaretIconDown(child);
    });
  }

  function collapseChildren(node) {
    traverseTree(node, function (child) {
      child.isExpanded = false;
      if (child.depth > node.depth) child.el.classList.add('hide');
      setCaretIconRight(child);
    });
  }

  exports.collapseChildren = collapseChildren;
  exports.createTree = createTree;
  exports.expandChildren = expandChildren;
  exports.render = render;
  exports.renderJSON = renderJSON;
  exports.traverseTree = traverseTree;

  return exports;

}({}));
