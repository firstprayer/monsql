from setuptools import setup, find_packages

setup(name='monsql',
      version='0.1.1',
      packages = find_packages(),
      author='firstprayer',
      author_email='zhangty10@gmail.com',
      url='https://github.com/firstprayer/python-mysql-wrapper.git',
      install_requires=[
      	'MySQL-python'
      ],
      # package_dir={'dbwrapper': 'dbwrapper'},
)
