ACLOCAL_AMFLAGS = -I m4
SUBDIRS = src

EXTRA_DIST = Doxyfile.in README

if HAVE_DOXYGEN
doxygen: Doxyfile
	$(DOXYGEN) Doxyfile

clean-local:
	rm -rf doc
endif
