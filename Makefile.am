ACLOCAL_AMFLAGS = -I m4
SUBDIRS = src

EXTRA_DIST = Doxyfile.in README cassobs.pc.in

if HAVE_DOXYGEN
doxygen: Doxyfile
	$(DOXYGEN) Doxyfile

clean-local:
	rm -rf doc
endif

pkgconfig_DATA = cassobs.pc.in

DISTCLEANFILES = $ (pkgconfig_DATA)
