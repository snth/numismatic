# Build and release instructions #

## Preparing a release ##

  * Update `VERSION.txt`

## Building wheels and uploading to PyPI ##

    python setup.py sdist bdist_wheel upload -r pypi

## Updating the README files ##

  * Edit the `README.md` file
  * Build the `README.rst` file with (requires `pandoc` to be installed):

    pandoc --from=markdown --to=rst --output=README.rst README.md
