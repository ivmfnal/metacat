FILES = __init__.py mql10.py sql_converter.py meta_evaluator.py query_executor.py
GRAMMAR_FILES = __init__.py combine.py dataset_query.py file_query.py common.py
GRAMDIR = $(MQLDIR)/grammar

build:
	mkdir -p $(MQLDIR)
	cp $(FILES) $(MQLDIR)
	mkdir -p $(GRAMDIR)
	cd grammar; cp $(GRAMMAR_FILES) $(GRAMDIR)
	
