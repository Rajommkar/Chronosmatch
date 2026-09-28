"""
Build the Cython matching engine as a C-extension:

    python3 setup.py build_ext --inplace

This compiles engine/matching_engine.pyx -> engine/matching_engine.c
-> engine/matching_engine.cpython-*.so, which is then importable as
`from engine.matching_engine import MatchingEngine`.
"""
from setuptools import setup
from Cython.Build import cythonize

setup(
    name="chronosmatch_engine",
    ext_modules=cythonize(
        "engine/matching_engine.pyx",
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
            "cdivision": True,
        },
        annotate=False,
    ),
    zip_safe=False,
)
