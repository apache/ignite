'use strict';

// Depends
var path = require('path');
var webpack = require('webpack');
var autoprefixer = require('autoprefixer-core');
// var Manifest = require('manifest-revision-webpack-plugin');
var ExtractTextPlugin = require("extract-text-webpack-plugin");

var NODE_ENV = process.env.NODE_ENV || "production";
var DEVELOPMENT = NODE_ENV === "production" ? false : true;
var stylesLoader = 'css-loader?sourceMap!postcss-loader!sass-loader?outputStyle=expanded&sourceMap=true&sourceMapContents=true';


module.exports = function (_path) {
    var rootAssetPath = _path + 'src';

    var webpackConfig = {
        node: {
            fs: 'empty'
        },
        // entry points
        entry: {
            vendor: _path + '/app/vendor.js',
            app: _path + '/app/index.js',
            polyfill: 'babel-polyfill'
        },

        // output system
        output: {
            path: 'dist',
            filename: '[name].js',
            publicPath: '/'
        },

        // resolves modules
        resolve: {
            extensions: ['', '.js'],
            modulesDirectories: ['node_modules', './'],
            alias: {
                _appRoot: path.join(_path, 'app'),
                _images: path.join(_path,  'app', 'assets', 'images'),
                _stylesheets: path.join(_path, 'app', 'assets', 'styles'),
                _scripts: path.join(_path,  'app', 'assets', 'js'),
                ace: path.join(_path, 'node_modules','ace-builds','src')
            }
        },

        // modules resolvers
        module: {
            noParse: [],
            // preLoaders: [
            //     {
            //         test: /\.js$/,
            //         exclude: [
            //             path.resolve(_path, "node_modules")
            //         ],
            //         loader: 'eslint-loader'
            //     }
            // ],
            loaders: [
                {
                    test: /\.json$/,
                    loader: 'json-loader'
                },
                {
                    test: /\.html$/,
                    loaders: [
                        'ngtemplate-loader?relativeTo=' + _path,
                        'html-loader?attrs[]=img:src&attrs[]=img:data-src'
                    ]
                },
                {
                    test: /\.jade$/,
                    loaders: [
                        'ngtemplate-loader?relativeTo=' + _path,
                        'html-loader?attrs[]=img:src&attrs[]=img:data-src',
                        'jade-html-loader'
                    ]
                }, {
                    test: /\.js$/,
                    loaders: [
                        'baggage-loader?[file].html&[file].css'
                    ]
                }, {
                    test: /\.js$/,
                    exclude: [
                        path.resolve(_path, "node_modules")
                    ],
                    loaders: [
                        'ng-annotate-loader'
                    ]
                }, {
                    test: /\.js$/,
                    exclude: [
                        path.resolve(_path, "node_modules")
                    ],
                    loader: 'babel-loader',
                    query: {
                        cacheDirectory: true,
                        plugins: ['transform-runtime', 'add-module-exports'],
                        presets: ['angular', 'es2017']
                    }
                }
                , {
                    test: /\.css$/,
                    loaders: [
                        'style-loader',
                        'css-loader?sourceMap',
                        'postcss-loader'
                    ]
                }, {
                    test: /\.(scss|sass)$/,
                    loader: DEVELOPMENT ? ('style-loader!' + stylesLoader) : ExtractTextPlugin.extract('style-loader', stylesLoader)
                }, {
                    test: /\.(woff2|woff|ttf|eot|svg)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
                    loaders: [
                        "url-loader?name=assets/fonts/[name]_[hash].[ext]"
                    ]
                }, {
                    test: /\.(jpe?g|png|gif)$/i,
                    loaders: [
                        'url-loader?name=assets/images/[name]_[hash].[ext]'
                    ]
                }, {
                    test: require.resolve("angular"),
                    loaders: [
                        "expose?angular"
                    ]
                    
                },

                {
                    test: require.resolve("jquery"),
                    loaders: [
                        "expose?$",
                        "expose?jQuery"
                    ]
                }

            ]
        },
        
        
        

        // post css
        postcss: [autoprefixer({browsers: ['last 2 versions']})],

        // load plugins
        plugins: [
            //new webpack.ContextReplacementPlugin(/moment[\/\\]locale$/, /en|hu/),
            new webpack.ProvidePlugin({
                $: 'jquery',
                jQuery: 'jquery',
                _: 'lodash'
            }),
            new webpack.DefinePlugin({
                'NODE_ENV': JSON.stringify(NODE_ENV)
            }),
            new webpack.NoErrorsPlugin(),
            new webpack.IgnorePlugin(/^\.\/locale$/, /moment$/),
            new webpack.optimize.DedupePlugin(),
            new webpack.optimize.AggressiveMergingPlugin({
                moveToParents: true
            }),
            new webpack.optimize.CommonsChunkPlugin({
                name: 'common',
                async: true,
                children: true,
                minChunks: Infinity
            }),
            // new Manifest(path.join(_path + '/config', 'manifest.json'), {
            //     rootAssetPath: rootAssetPath,
            //     ignorePaths: ['.DS_Store']
            // }),
            new ExtractTextPlugin('assets/styles/css/[name]' + (NODE_ENV === 'development' ? '' : '.[chunkhash]') + '.css', {allChunks: true})
        ]
    };

    if (NODE_ENV !== 'development') {
        webpackConfig.plugins = webpackConfig.plugins.concat([
            new webpack.optimize.UglifyJsPlugin({
                minimize: true,
                warnings: false,
                sourceMap: true
            })
        ]);
    }

    return webpackConfig;

};
