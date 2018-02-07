from setuptools import setup


setup(
    name='crosstab',
    version='0.1',
    description='New grammar for SQLAlchemy to make handling the crosstab() tablefunc (i.e. pivot tables) in Postgresql easy peasy',
    long_description='',
    # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'License :: OSI Approved :: BSD License',
    ],
    keywords='',
    author='Mehmet Ali "Mali" Akmanalp',
    author_email='',
    url='https://github.com/makmanalp/sqlalchemy-crosstab-postgresql',
    license='BSD license',
    py_modules=['crosstab'],
    zip_safe=False,
    install_requires=[
        'sqlalchemy',
        'pytest',
        'psycopg2'
    ],
)
