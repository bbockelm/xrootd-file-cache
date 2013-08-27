bdir = $$HOME/rpmbuild/SOURCES
rname = xrootd-file-cache



clean :
	cd $(bdir); rm -rf  $(bdir)/$(rname)

tarball : clean
	cd $(bdir); git clone https://github.com/alja/$(rname)
	cd $(bdir)/$(rname);  git archive --format=tar master | gzip > $(bdir)/$(rname).tar.gz

rpm:    tarball
	cp $(bdir)/$(rname)/spec/$(rname).spec  $(bdir)/$(rname).spec
	rpmbuild -bs $(bdir)/$(rname).spec
	rpmbuild -ba $(bdir)/$(rname).spec

all : rpm
	echo "Building rpm ..."

