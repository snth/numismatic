How To Contribute
=================

First off, thank you for considering contributing to ``Numismatic``!
It's people like *you* who make it is such a great tool for everyone.

This document is mainly to help you to get started by codifying tribal knowledge and expectations and make it more accessible to everyone.
But don't be afraid to open half-finished PRs and ask questions if something is unclear!

Workflow
--------

- No contribution is too small!
  Please submit as many fixes for typos and grammar bloopers as you can!
- Try to limit each pull request to *one* change only.
- *Always* add tests and docs for your code.
  This is a hard rule; patches with missing tests or documentation can't be merged.
- Make sure your changes pass our CI_.
  You won't get any feedback until it's green unless you ask for it.
- Once you've addressed review feedback, make sure to bump the pull request with a short note, so we know you're done.
- Don’t break `backward compatibility`_.


Code
----

- Obey `PEP 8`_ and `PEP 257`_.
  We use the ``"""``\ -on-separate-lines style for docstrings:

  .. code-block:: python

     def func(x):
         """
         Do something.

         :param str x: A very important parameter.

         :rtype: str
         """
- If you add or change public APIs, tag the docstring using ``..  versionadded:: 16.0.0 WHAT`` or ``..  versionchanged:: 16.2.0 WHAT``.
- Prefer double quotes (``"``) over single quotes (``'``) unless the string contains double quotes itself.


Tests
-----

- Write your asserts as ``expected == actual`` to line them up nicely:

  .. code-block:: python

     x = f()

     assert 42 == x.some_attribute
     assert "foo" == x._a_private_attribute

- To run the test suite, all you need is a recent tox_.
  It will ensure the test suite runs with all dependencies against all Python versions just as it will on Travis CI.
  If you lack some Python versions, you can can always limit the environments like ``tox -e py27,py35`` (in that case you may want to look into pyenv_, which makes it very easy to install many different Python versions in parallel).
- Write `good test docstrings`_.
- To ensure new features work well with the rest of the system, they should be also added to our `Hypothesis`_ testing strategy which you find in ``tests/util.py``.


Documentation
-------------

- Use `semantic newlines`_ in reStructuredText_ files (files ending in ``.rst``):

  .. code-block:: rst

     This is a sentence.
     This is another sentence.

- If you start a new section, add two blank lines before and one blank line after the header except if two headers follow immediately after each other:

  .. code-block:: rst

     Last line of previous section.


     Header of New Top Section
     -------------------------

     Header of New Section
     ^^^^^^^^^^^^^^^^^^^^^

     First line of new section.
- If you add a new feature, demonstrate its awesomeness in the `examples page`_!


Changelog
^^^^^^^^^

If your change is noteworthy, there needs to be a changelog entry, so our users can learn about it!

To avoid merge conflicts, we use the towncrier_ package to manage our changelog.
``towncrier`` uses independent files for each pull request -- so called *news fragments* -- instead of one monolithic changelog file.
On release those news fragments are compiled into our ``CHANGELOG.rst``.

You don't need to install ``towncrier`` yourself, you just have to abide to a few simple rules:

- For each pull request, add a new file into ``changelog.d`` with a filename adhering to the ``pr#.(change|deprecation|breaking).rst`` schema:
  For example ``changelog.d/42.change.rst`` for a non-breaking change, that is proposed in pull request number 42.
- As with other docs, please use `semantic newlines`_ within news fragments.
- Wrap symbols like modules, functions, or classes into double backticks so they are rendered in a monospaced font.
- If you mention functions or other callables, add parantheses at the end of their names: ``attr.func()`` or ``attr.Class.method()``.
  This makes the changelog a lot more readable.
- Prefer simple past or constructions with "now".
  For example:

  + Added ``attr.validators.func()``.
  + ``attr.func()`` now doesn't crash the Large Hadron Collider anymore.
- If you want to reference multiple issues, copy the news fragment to another filename.
  ``towncrier`` will merge all news fragments with identical contents into one entry with multiple links to the respective pull requests.

Example entries:

  .. code-block:: rst

     Added ``attr.validators.func()``.
     The feature really *is* awesome.

or:

  .. code-block:: rst

     ``attr.func()`` now doesn't crash the Large Hadron Collider anymore.
     The bug really *was* nasty.

----

``tox -e changelog`` will render the current changelog to the terminal if you have any doubts.


Local Development Environment
-----------------------------



Governance
----------

``Numismatic`` is maintained by `team of volunteers`_ that is always open for new members that share our vision of a fast, lean, and magic-free library that empowers programmers to write better code with less effort.
If you'd like to join, just get a pull request merged and ask to be added in the very same pull request!

**The simple rule is that everyone is welcome to review/merge pull requests of others but nobody is allowed to merge their own code.**

`Hynek Schlawack`_ acts reluctantly as the BDFL_ and has the final say over design decisions.


****

Please note that this project is released with a Contributor `Code of Conduct`_.
By participating in this project you agree to abide by its terms.
Please report any harm to `Hynek Schlawack`_ in any way you find appropriate.

Thank you for considering contributing to ``Numismatic``!
