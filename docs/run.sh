#!/bin/bash


rm -rf _site
rm -rf .jekyll-cache

bundle exec jekyll s -lI --force_polling --trace
