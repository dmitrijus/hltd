#!/usr/bin/env python

import distutils.core
import distutils.util

platform = distutils.util.get_platform()


distutils.core.setup(
    name='procname',
    version='0.1',
    description='Process name renaming',
    author="Eugene A Lisitsky",
    license='LGPL',
    platforms='Linux',
    ext_modules=[distutils.core.Extension('procname', sources=['procnamemodule.c'])],
    )
