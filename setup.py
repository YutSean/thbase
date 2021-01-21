from setuptools import setup, find_packages
from os import path
here = path.abspath(path.dirname(__file__))
setup(
    name='pythbase',
    version='1.0.0',
    description='HBase thrift client python API',
    long_description='Provides put, get, scan, delete operations.',
    # Author details
    author='Sean',
    author_email='sean.xiaoyt@shopee.com',
    # Choose your license
    classifiers=[
        'Development Status :: 1 - first version',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2.7, 3.6',
    ],
    packages=find_packages(),
    py_modules=["pythbase"],
    install_requires=["thrift==0.13.0", "enum34", "typing"]
)
