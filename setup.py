from setuptools import setup, find_packages
 
setup(
    name='Unicode CSV',
    version='0.1',
    packages = find_packages('src'),
    package_dir = {'': 'src'},
    zip_safe=True,
)

