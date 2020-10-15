PRODUCT=metacat
VERSION=1.4
BUILD_DIR=$(HOME)/build/$(PRODUCT)
TARDIR=/tmp
LIBDIR=$(BUILD_DIR)/lib
SERVER_DIR=$(BUILD_DIR)/server
DOCSDIR=$(BUILD_DIR)/docs
DOCSREL=$(BUILD_DIR)/docs
UI_DIR=$(BUILD_DIR)/ui
SERVER_TAR=$(TARDIR)/$(PRODUCT)_server_$(VERSION).tar


all:    tars

tars:   build $(TARDIR)
	cd $(BUILD_DIR); tar cf $(SERVER_TAR) lib server docs
	@echo \|
	@echo \| tarfile is created: $(SERVER_TAR)
	@echo \|
	

build:  clean $(BUILD_DIR) 
	cd src; make LIBDIR=$(LIBDIR) VERSION=$(VERSION) build
	cd lib; make LIBDIR=$(LIBDIR) VERSION=$(VERSION) build
	cd webserver; make SERVER_DIR=$(SERVER_DIR) LIBDIR=$(LIBDIR) VERSION=$(VERSION) build
	cd docs; make SERVER_DIR=$(SERVER_DIR) DOCSDIR=$(DOCSDIR) -f Makefile-product build
	cd ui; make UI_DIR=$(UI_DIR) build
	
clean:
	rm -rf $(BUILD_DIR) $(SERVER_TAR)
	
$(TARDIR):
	mkdir -p $@
	

$(BUILD_DIR):
	mkdir -p $@
	

	
