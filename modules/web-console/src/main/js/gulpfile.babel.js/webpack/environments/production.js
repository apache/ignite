'use strict';
var CleanWebpackPlugin = require('clean-webpack-plugin');
var HtmlWebpackPlugin = require("html-webpack-plugin");
var path = require('path');

module.exports = function(_path) {
  return {
    context: _path,
    debug: false,
    devtool: 'cheap-source-map',
    output: {
      publicPath: '/',
      filename: '[name].[chunkhash].js'
    },
    plugins: [
      new CleanWebpackPlugin(['dist'], {
        root: _path,
        verbose: true,
        dry: false
      }),
      new HtmlWebpackPlugin({
        filename: 'index.html',
        template: path.join(_path, 'views', 'index.html'),
        title: 'Ignite Web Console',
        favicon: path.join(_path, 'src', 'favicon.ico')
      })
    ]
  };
};