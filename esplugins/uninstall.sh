#!/bin/bash
cd $1
echo uninstalling elastic plugin $2 ...
bin/plugin -s --remove $2

