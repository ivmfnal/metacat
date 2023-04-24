FILES = __init__.py version.py
DBFILES = __init__.py dbobjects2.py common.py param_category.py
WEBAPIFILES = __init__.py webapi.py
FILTERSFILES = __init__.py filters.py
MODULEDIR=$(LIBDIR)/metacat

build:
	mkdir -p $(MODULEDIR) $(MODULEDIR)/db $(MODULEDIR)/mql $(MODULEDIR)/util $(MODULEDIR)/webapi $(MODULEDIR)/filters $(MODULEDIR)/common $(MODULEDIR)/logs
	cp $(FILES) $(MODULEDIR)
	cd db; cp $(DBFILES) $(MODULEDIR)/db
	cd ui; make LIBDIR=$(MODULEDIR)/ui BINDIR=$(BINDIR) build
	cd webapi; cp $(WEBAPIFILES) $(MODULEDIR)/webapi
	cd filters; cp $(FILTERSFILES) $(MODULEDIR)/filters
	cd mql; make MQLDIR=$(MODULEDIR)/mql build
	cp -R auth common $(MODULEDIR)
	cp util/*.py $(MODULEDIR)/util
	cp logs/*.py $(MODULEDIR)/logs
	
	
