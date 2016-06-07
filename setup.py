"""
setup.py

CDS Integration Sample
"""

from setuptools import setup, find_packages

requirements_file = open('requirements.txt')
requirements = requirements_file.read().strip().split('\n')

setup_args = {
    'description': 'CDS Integration Sample',
    'include_package_data': True,
    'install_requires': requirements,
    'name': 'sample',
    'version': '0.0.1',
    'packages': find_packages('./src'),
    'provides': find_packages('./src'),
    'package_dir': {'': 'src'}
}

setup(**setup_args)
