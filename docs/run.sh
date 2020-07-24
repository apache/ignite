#!/bin/bash


rm -rf _site
rm -rf .jekyll-cache

bundle exec jekyll s -I --trace
