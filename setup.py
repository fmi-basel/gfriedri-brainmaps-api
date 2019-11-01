import setuptools

authors = [
    'Nila Moenig',
]

description = 'python functions for interacting with the Google Brainmaps API'

setuptools.setup(
    name='brainmaps_api_fcn',
    version='0.0.1',
    author=authors,
    packages=setuptools.find_packages(),
    description=description,
    long_description=open('README.md').read(),
    install_requires=[
        'google-auth>=1.6.3',
        'google-auth-oauthlib>=0.3.0',
        'networkx>=2.3',
        'numpy',
        'requests'
    ],
)