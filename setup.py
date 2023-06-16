import os
import sys
from setuptools import setup, find_packages


package_basename = 'desipipe'
package_dir = os.path.join(os.path.dirname(__file__), package_basename)
sys.path.insert(0, package_dir)
import _version
version = _version.__version__


setup(name=package_basename,
      version=version,
      author='cosmodesi',
      author_email='',
      description='Package for DESI clustering analysis pipeline',
      license='BSD3',
      url='http://github.com/cosmodesi/desipipe',
      install_requires=['pyyaml', 'mpi4py'],
      extras_require={},
      packages=find_packages(),
      entry_points={'console_scripts': ['desipipe = desipipe.__main__:main', 'desipipe-mpispawn = desipipe.provider:mpi_spawn'],
    })
