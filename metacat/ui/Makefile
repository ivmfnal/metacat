BINFILES = metacat 	query_test.py
LIBFILES = \
    metacat_ui.py metacat_config.py  \
    metacat_auth.py metacat_dataset.py metacat_query.py metacat_file.py metacat_namespace.py \
	metacat_admin.py 

build:	
	mkdir -p $(BINDIR) $(LIBDIR)
	cp $(BINFILES) $(BINDIR); cd $(BINDIR); chmod +x $(BINFILES)
	cp $(LIBFILES) $(LIBDIR)
	
