"""A setuptools based setup module.

See:
https://packaging.python.org/guides/distributing-packages-using-setuptools/
https://github.com/pypa/anvil
"""


import setuptools.command.build_py
# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from os import path
import subprocess


class DownloadDataIngestionSpreadsheetCommand(setuptools.command.build_py.build_py):
    """Custom build command."""

    description = 'download data from AnVIL data ingestion spreadsheet'

    def run(self):
        """Run the command."""
        self.announce('running data_ingestion_tracker')
        process = subprocess.run(['bin/data_ingestion_tracker'],
                                 stdout=subprocess.PIPE,
                                 universal_newlines=True)
        assert process.returncode == 0, "data_ingestion_tracker must execute successfully"


here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    # This is the name of your project. The first time you publish this
    # package, this name will be registered for you. It will determine how
    # users can install this project, e.g.:
    #
    # $ pip install pyAnVIL
    #
    # And where it will live on PyPI: https://pypi.org/project/pyAnVIL/
    #
    # There are some restrictions on what makes a valid project name
    # specification here:
    # https://packaging.python.org/specifications/core-metadata/#name
    name='pyAnVIL',  # Required

    # Versions should comply with PEP 440:
    # https://packaging.python.org/en/latest/single_source_version.html
    version='0.0.6rc6',  # Required

    # This is a one-line description or tagline of what your project does. This
    # corresponds to the "Summary" metadata field:
    # https://packaging.python.org/specifications/core-metadata/#summary
    description='AnVIL client library. Combines gen3, terra client APIs with single signon and data harmonization use cases.',  # Optional

    # This field corresponds to the "Description" metadata field:
    # https://packaging.python.org/specifications/core-metadata/#description-optional
    long_description=long_description,  # Optional

    # Denotes that our long_description is in Markdown
    long_description_content_type='text/markdown',  # Optional (see note above)

    # Project's main homepage.
    url='https://github.com/anvilproject/client-apis',  # Optional

    # This should be your name or the name of the organization which owns the
    # project.
    author='The AnVIL project',  # Optional

    # This should be a valid email address corresponding to the author listed
    # above.
    author_email='walsbr@ohsu.edu',  # Optional

    cmdclass={
        'data_ingestion_tracker': DownloadDataIngestionSpreadsheetCommand,
    },


    # For a list of valid classifiers, see https://pypi.org/classifiers/
    classifiers=[  # Optional
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here.
        'Programming Language :: Python :: 3.7',
    ],

    # This field adds keywords for your project which will appear on the
    # project page. What does your project relate to?
    #
    # Note that this is a string of words separated by whitespace, not a list.
    keywords='AnVIL terra gen3 bioinformatics',  # Optional

    # You can just specify package directories manually here if your project is
    # simple. Or you can use find_packages().
    #
    # Alternatively, if you just want to distribute a single Python file, use
    # the `py_modules` argument instead as follows, which will expect a file
    # called `my_module.py` to exist:
    #
    #   py_modules=["my_module"],
    #
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),  # Required

    # Specify which Python versions you support. In contrast to the
    # 'Programming Language' classifiers above, 'pip install' will check this
    # and refuse to install the project if the version does not match. If you
    # do not support Python 2, you can simplify this to '>=3.5' or similar, see
    # https://packaging.python.org/guides/distributing-packages-using-setuptools/#python-requires
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, <4',

    # This field lists other packages that your project depends on to run.
    # Any package you put here will be installed by pip when your project is
    # installed, so they must be valid existing projects.
    #
    # For an analysis of "install_requires" vs pip's requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=[
        'gen3==2.4.0',
        'firecloud==0.16.29',
        'xmltodict==0.12.0',
        'Click==7.0',
        'attrdict==2.0.1',
        'google-cloud-storage==1.19.1',
        'fastavro==1.2.0'
    ],

    # List additional groups of dependencies here (e.g. development
    # dependencies). Users will be able to install these using the "extras"
    # syntax, for example:
    #
    #   $ pip install anvil[dev]
    #
    # Similar to `install_requires` above, these must be valid existing
    # projects.
    # extras_require={  # Optional
    #     'dev': ['check-manifest'],
    #     'test': ['coverage'],
    # },

    # If there are data files included in your packages that need to be
    # installed, specify them here.
    #
    # If using Python 2.6 or earlier, then these have to be included in
    # MANIFEST.in as well.
    # package_data={  # Optional
    #     'sample': ['package_data.dat'],
    # },

    # Although 'package_data' is the preferred approach, in some case you may
    # need to place data files outside of your packages. See:
    # http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files
    #
    # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
    # data_files=[('my_data', ['data/data_file'])],  # Optional

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # `pip` to create the appropriate form of executable for the target
    # platform.
    #
    # For example, the following would provide a command called `sample` which
    # executes the function `main` from this package when invoked:
    # entry_points={  # Optional
    #     'console_scripts': [
    #         'sample=sample:main',
    #     ],
    # },

    # TODO - not working
    # entry_points={
    #     'console_scripts': [
    #         'data_ingestion_tracker=anvil.bin.data_ingestion_tracker:data_ingestion_tracker',
    #         'reconciler=anvil.bin.reconciler:reconciler',
    #     ],
    # },
    scripts=['bin/data_ingestion_tracker', 'bin/reconciler'],
    include_package_data=True,

    # List additional URLs that are relevant to your project as a dict.
    #
    # This field corresponds to the "Project-URL" metadata fields:
    # https://packaging.python.org/specifications/core-metadata/#project-url-multiple-use
    #
    # Examples listed include a pattern for specifying where the package tracks
    # issues, where the source is hosted, where to say thanks to the package
    # maintainers, and where to support the project financially. The key is
    # what's used to render the link text on PyPI.
    project_urls={  # Optional
        'Bug Reports': 'https://github.com/anvilproject/client-apis/issues',
        'Source': 'https://github.com/anvilproject/client-apis/',
    },
)
