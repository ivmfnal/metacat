FILES = __init__.py version.py
DBFILES = __init__.py dbobjects2.py common.py param_category.py
UTILFILES = __init__.py py3.py rfc2617.py signed_token_jwt.py token_box.py token_lib.py timelib.py password_hash.py authenticators.py
WEBAPIFILES = __init__.py webapi.py
FILTERSFILES = __init__.py filters.py
MODULEDIR=$(LIBDIR)/metacat

build:
	mkdir -p $(MODULEDIR) $(MODULEDIR)/db $(MODULEDIR)/mql $(MODULEDIR)/util $(MODULEDIR)/webapi $(MODULEDIR)/filters
	cp $(FILES) $(MODULEDIR)
	cd db; cp $(DBFILES) $(MODULEDIR)/db
	cd ui; make LIBDIR=$(MODULEDIR)/ui BINDIR=$(BINDIR) build
	cd util; cp $(UTILFILES) $(MODULEDIR)/util
	cd webapi; cp $(WEBAPIFILES) $(MODULEDIR)/webapi
	cd filters; cp $(FILTERSFILES) $(MODULEDIR)/filters
	cd mql; make MQLDIR=$(MODULEDIR)/mql build
	
	
