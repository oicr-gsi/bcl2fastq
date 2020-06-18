# Define an empty variable for list termination
NULL = 
ACLOCAL_AMFLAGS = -I m4

# Build the following binaries and put the in $prefix/bin
bin_PROGRAMS = \
	bcl2fastq-jail \
	$(NULL)
# Install the following manpages in $prefix/share/man/man1
man1_MANS = \
	bcl2fastq-jail.1 \
	$(NULL)

# These are all the source .cpp files for. Note the change of `-` to `_`.
bcl2fastq_jail_SOURCES = \
	main.cpp \
	$(NULL)
# These are the compiler arguments for building
# We want (in order):
# - C++14
# - debugging symbols to be included
# - optimisation level 2
# - whatever junk jsoncpp wants
bcl2fastq_jail_CPPFLAGS = \
	-std=c++14 -g -O2 \
	$(JSON_CFLAGS) \
	$(PTHREAD_CFLAGS) \
	$(ZLIB_CFLAGS) \
	$(NULL)
# These are the linker flags for building
# We need:
# - whatever junk jsoncpp needs
bcl2fastq_jail_LDADD = \
  -lgzstream \
	$(JSON_LIBS) \
	$(PTHREAD_CFLAGS) \
	$(PTHREAD_LIBS) \
	$(ZLIB_LIBS) \
	$(NULL)
