FILES = AuthServer.py Server.py rfc2617.py \
    base_handler.py auth_handler.py gui_handler.py data_handler.py \
    base.html dataset_files.html dataset.html datasets.html login.html mql.html named_queries.html named_query.html \
    namespace.html namespaces.html query.html role.html roles.html show_file.html user.html users.html \
	categories.html category.html



build:	$(SERVER_DIR)
	cp $(FILES) $(SERVER_DIR)
	mkdir -p $(SERVER_DIR)/static/js $(SERVER_DIR)/static/images
	cp static/js/*.js $(SERVER_DIR)/static/js
	cp static/images/*.png static/images/*.jpg $(SERVER_DIR)/static/images
	cp -R static/fonts $(SERVER_DIR)/static
	
$(SERVER_DIR):
	mkdir -p $@
