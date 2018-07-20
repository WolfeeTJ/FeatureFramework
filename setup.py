from setuptools import setup, find_packages
import compileall

additional_scripts=[
    '1_ff_data_check.py',
    '2_ff_var_generate.py',
]

compileall.compile_dir(".", legacy=True)

with open("README.md", "r", encoding="UTF8") as fh:
    long_description = fh.read()

setup(
    name="CreditLife_FeatureFramework",
    version="0.1",
    packages=find_packages(),
    scripts=additional_scripts,

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires=['docutils>=0.3'],

    package_data={
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.txt', '*.rst'],
        # And include any *.msg files found in the 'hello' package, too:
        # "CreditLife_FeatureFramework": ['*.pyc'],
    },
    exclude_package_data={'': ["*.py"]},

    # metadata to display on PyPI
    author="Du Guang",
    author_email="duguang@wecreditlife.com",
    description="CreditLife Feature Framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="Property",
    keywords="CreditLife Feature Framework",
    url="http://wecreditlife.com",   # project home page, if any

    # could also include long_description, download_url, classifiers, etc.
)