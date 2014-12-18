from setuptools import setup, find_packages

setup(name='monsql',
      version='0.1.4',
      packages = find_packages(),
      author='firstprayer',
      author_email='zhangty10@gmail.com',
      url='https://github.com/firstprayer/monsql.git',
      install_requires=[
      	'MySQL-python'
      ],
)
