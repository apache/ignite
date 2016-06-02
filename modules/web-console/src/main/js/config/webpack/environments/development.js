'use strict';
var webpack = require('webpack');
var HtmlWebpackPlugin = require("html-webpack-plugin");
var path = require('path');

module.exports = function(_path) {
  return {
    context: _path,
    debug: true,
    devtool: 'cheap-source-map',
    devServer: {
      contentBase: './dist',
      info: true,
      hot: true,
      inline: true
    },
    plugins: [
      new webpack.HotModuleReplacementPlugin(),
      new HtmlWebpackPlugin({
        filename: 'index.html',
        template: path.join(_path, 'views', 'index.html'),
        title: 'DEBUG:Ignite Web Console'
      })
    ]
  };
};
