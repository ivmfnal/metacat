PRODUCT=metacat
BUILD_DIR=$(HOME)/build/$(PRODUCT)
TARDIR=/tmp
LIBDIR=$(BUILD_DIR)/lib
MODULEDIR=$(LIBDIR)/metacat
SERVER_DIR=$(BUILD_DIR)/server
DOCSDIR=$(BUILD_DIR)/docs
DOCSREL=$(BUILD_DIR)/docs
UI_DIR=$(BUILD_DIR)/ui
SERVER_TAR=$(TARDIR)/$(PRODUCT)_server_$(VERSION).tar

all:
	make VERSION=`python metacat/version.py` all_with_version_defined
	
clean:
	make VERSION=`python metacat/version.py` clean_with_version_defined

clean_with_version_defined:
	rm -rf $(BUILD_DIR) $(TAR_FILE)

all_with_version_defined:	tars

tars:   build $(TARDIR)
	cd $(BUILD_DIR); tar cf $(SERVER_TAR) lib server docs
	@echo \|
	@echo \| tarfile is created: $(SERVER_TAR)
	@echo \|
	

build:  clean $(BUILD_DIR) 
	cd src; make LIBDIR=$(LIBDIR) VERSION=$(VERSION) build
	cd metacat; make LIBDIR=$(LIBDIR) VERSION=$(VERSION) BINDIR=$(UI_DIR) build
	cd webserver; make SERVER_DIR=$(SERVER_DIR) LIBDIR=$(LIBDIR) VERSION=$(VERSION) build
	cd docs; make SERVER_DIR=$(SERVER_DIR) DOCSDIR=$(DOCSDIR) -f Makefile-product build
	
clean:
	rm -rf $(BUILD_DIR) $(SERVER_TAR)
	
$(TARDIR):
	mkdir -p $@
	

$(BUILD_DIR):
	mkdir -p $@
	

	
