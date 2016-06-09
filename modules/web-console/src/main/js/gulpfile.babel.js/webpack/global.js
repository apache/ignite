'use strict';

import path from 'path';
import webpack from 'webpack';
import autoprefixer from 'autoprefixer-core';
//import  Manifest from 'manifest-revision-webpack-plugin';
import ExtractTextPlugin from 'extract-text-webpack-plugin';
import {srcDir, destDir, rootDir} from '../paths';

const NODE_ENV = process.env.NODE_ENV || 'production';
const IS_DEVELOPMENT = NODE_ENV === 'development';
const stylesLoader = 'css-loader?sourceMap!postcss-loader!sass-loader?outputStyle=expanded&sourceMap=true&sourceMapContents=true';


module.exports = function (_path) {

    const NODE_MODULES_PATH = path.resolve('node_modules');
    const webpackConfig = {
        node: {
            fs: 'empty'
        },
        // entry points
        entry: {
            polyfill: 'babel-polyfill',
            app: srcDir + '/index.js',
            vendor: srcDir + '/vendor.js',
        },

        // // output system
        output: {
            path: destDir,
            filename: '[name].js'
        },

        // resolves modules
        resolve: {
            extensions: ['', '.js'],
            root : rootDir,
            modulesDirectories: [NODE_MODULES_PATH, './'],
            alias: {
                // _appRoot: path.join(_path, 'app'),
                // _images: path.join(_path, 'app', 'assets', 'images'),
                // _stylesheets: path.join(_path, 'app', 'assets', 'styles'),
                // _scripts: path.join(_path, 'app', 'assets', 'js'),
                // ace: path.join(_path, 'node_modules', 'ace-builds', 'src')
            }
        },

        // modules resolvers
        module: {
            noParse: [],
            // preLoaders: [
            //     {
            //         test: /\.js$/,
            //         exclude: [NODE_MODULES_PATH],
            //         loader: 'eslint-loader'
            //     }
            // ],
            loaders: [
                {
                    test: /\.json$/,
                    loader: 'json-loader'
                },
                {
                    test: /\.jade$/,
                    loaders: [
                        'ngtemplate-loader?relativeTo=' + _path,
                        'html-loader?attrs[]=img:src&attrs[]=img:data-src',
                        'jade-html-loader'
                    ]
                },
                // {
                //     test: /\.js$/,
                //     loaders: ['baggage-loader?[file].html&[file].css']
                // },
                // {
                //     test: /\.js$/,
                //     exclude: [NODE_MODULES_PATH],
                //     loaders: ['ng-annotate-loader']
                // },
                {
                    test: /\.js$/,
                    exclude: [NODE_MODULES_PATH],
                    loader: 'babel-loader',
                    query: {
                        cacheDirectory: true,
                        plugins: ['transform-runtime', 'add-module-exports'],
                        presets: ['angular', 'es2017']
                    }
                },
                {
                    test: /\.css$/,
                    loaders: ['style-loader', 'css-loader?sourceMap', 'postcss-loader']
                },
                {
                    test: /\.(scss|sass)$/,
                    loader: IS_DEVELOPMENT ? ('style-loader!' + stylesLoader) : ExtractTextPlugin.extract('style-loader', stylesLoader)
                },
                {
                    test: /\.(woff2|woff|ttf|eot|svg)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
                    loaders: [
                        'url-loader?name=fonts/[name]_[hash].[ext]'
                    ]
                },
                {
                    test: /\.(jpe?g|png|gif)$/i,
                    loaders: ['url-loader?name=images/[name]_[hash].[ext]']
                }
                // {
                //     test: require.resolve("jquery"),
                //     loaders: [
                //         "expose?$",
                //         "expose?jQuery"
                //     ]
                // },
                // {
                //     test: require.resolve("angular"),
                //     loaders: [
                //         "expose?angular"
                //     ]
                // }
            ]
        },

        // post css
        postcss: [autoprefixer({browsers: ['last 2 versions']})],

        // load plugins
        plugins: [
            new webpack.ProvidePlugin({
                $: 'jquery',
                jQuery: 'jquery',
                _: 'lodash',
                nv: 'nvd3'
            }),
            new webpack.DefinePlugin({'NODE_ENV': JSON.stringify(NODE_ENV)}),
            new webpack.NoErrorsPlugin(),
            new webpack.optimize.AggressiveMergingPlugin({moveToParents: true}),
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
            new ExtractTextPlugin('assets/css/[name]' + (IS_DEVELOPMENT ? '' : '.[chunkhash]') + '.css', {allChunks: true})
        ]
    };

    if (!IS_DEVELOPMENT) {
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
