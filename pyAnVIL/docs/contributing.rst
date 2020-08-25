Contributing
============

When contributing to this repository, please first discuss the change
you wish to make via issue, email, or any other method with the owners
of this repository before making a change.

Please note we have a code of conduct, please follow it in all your
interactions with the project.

Pull Request Process
--------------------

1. Ensure miscellaneous commits are squashed prior to merge.
2. Update the README.md with details of changes to the interface, this
   includes new environment variables, exposed ports, useful file
   locations and container parameters.
3. Increase the version numbers in any examples files and the README.md
   to the new version that this Pull Request would represent. The
   versioning scheme we use is `SemVer`_.
4. You may merge the Pull Request in once you have the sign-off of two
   other developers, or if you do not have permission to do that, you
   may request the second reviewer to merge it for you.


Tools
-------

1. A pre-commit hook will run flake8 and checks for large-files, credentials, and private keys.
2. Travis will run on commits.
3. Tests are standard pytests.  A service account configuration is in progress to automate integration tests.
