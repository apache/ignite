'use strict';
var HtmlWebpackPlugin = require("html-webpack-plugin");
var path = require('path');
var jade = require('jade');

module.exports = function (_path) {
    return {
        context: _path,
        debug: false,
        devtool: 'cheap-source-map',
        output: {
            publicPath: '/',
            filename: '[name].[chunkhash].js',
            contentBase: './dist'
        },
        plugins: [
            new HtmlWebpackPlugin({
                filename: 'index.html',
                templateContent: function () {
                    return jade.renderFile(path.join(_path, 'views', 'index.jade'));
                },
                title: 'Ignite Web Console'
            })
        ]
    };
};