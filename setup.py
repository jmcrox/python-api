#!/usr/bin/env python

#
# Python SSAP API
# Version 1.5
# 
# (C) Indra Sistemas, S.A.
# 2014  SPAIN
#  
# All rights reserved
#

from distutils.core import setup
from pkgutil import walk_packages
import os

setup(name="ssap",
      version="1.5",
      description="Python implementation of the Sofia2 SSAP API",
      author="Indra Sistemas S.A.",
      author_email="plataformasofia2@indra.es",
      url="http://www.sofia2.org",
      packages=["ssap", "ssap.implementations", "ssap.utils", "ssap.messages", "ssap.tests.utils", "ssap.tests.websockets"],
      package_dir = {"" : "src"}
     )
