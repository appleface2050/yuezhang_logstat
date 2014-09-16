from distutils.core import setup, Extension
example_mod = Extension('mytea', sources = ['teamodule.c'])
setup(name = "mytea",
	version = "1.0",
	description = "TEA extension C module",
	ext_modules = [example_mod],
)

