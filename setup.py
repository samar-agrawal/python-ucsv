from setuptools import setup, find_packages
 
setup(
    name='ucsv',
    version='0.1',
    packages = find_packages('.'),
    package_dir = {'': '.'},
    zip_safe=True,
)

