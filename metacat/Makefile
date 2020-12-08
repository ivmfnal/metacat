FILES = __init__.py version.py
DBFILES = __init__.py dbobjects2.py
MQLFILES = __init__.py grammar10.py mql10.py trees.py sql_converter.py meta_evaluator.py
UTILFILES = __init__.py py3.py rfc2617.py signed_token.py token_lib.py timelib.py password_hash.py
WEBAPIFILES = __init__.py webapi.py
FILTERSFILES = __init__.py filters.py
MODULEDIR=$(LIBDIR)/metacat

build:
	mkdir -p $(MODULEDIR) $(MODULEDIR)/db $(MODULEDIR)/mql $(MODULEDIR)/util $(MODULEDIR)/webapi $(MODULEDIR)/filters
	cp $(FILES) $(MODULEDIR)
	cd db; cp $(DBFILES) $(MODULEDIR)/db
	cd ui; make LIBDIR=$(MODULEDIR)/ui BINDIR=$(BINDIR) build
	cd mql; cp $(MQLFILES) $(MODULEDIR)/mql
	cd util; cp $(UTILFILES) $(MODULEDIR)/util
	cd webapi; cp $(WEBAPIFILES) $(MODULEDIR)/webapi
	cd filters; cp $(FILTERSFILES) $(MODULEDIR)/filters
	
	
