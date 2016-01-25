#!/bin/bash
cd $1
echo uninstalling elastic plugin $2 ...
#support 1.X and 2.X syntax:
bin/plugin -s --remove $2 || true
bin/plugin remove $2 -s || true

