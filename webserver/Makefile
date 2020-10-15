FILES = Server.py rfc2617.py \
    base.html dataset_files.html dataset.html datasets.html login.html mql.html named_queries.html named_query.html \
    namespace.html namespaces.html query.html role.html roles.html show_file.html user.html users.html



build:	$(SERVER_DIR)
	cp $(FILES) $(SERVER_DIR)
	mkdir -p $(SERVER_DIR)/static/js
	cp static/js/*.js $(SERVER_DIR)/static/js
	
$(SERVER_DIR):
	mkdir -p $@
