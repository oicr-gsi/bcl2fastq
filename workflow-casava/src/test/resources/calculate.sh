#!/bin/bash

cd $1
find . -type f ! -name "*.zip" -exec md5sum {} +
