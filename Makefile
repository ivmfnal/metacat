PRODUCT=metacat
BUILD_DIR=$(HOME)/build/$(PRODUCT)
TARDIR=/tmp/$(USER)
LIBDIR=$(BUILD_DIR)/lib
MODULEDIR=$(LIBDIR)/metacat
SERVER_DIR=$(BUILD_DIR)/server
DOCSDIR=$(BUILD_DIR)/docs
DAEMONDIR=$(BUILD_DIR)/daemon
DOCSREL=$(BUILD_DIR)/docs
UI_DIR=$(BUILD_DIR)/ui
DEPS_DIR=$(BUILD_DIR)/dependencies
SERVER_TAR=$(TARDIR)/$(PRODUCT)_server_$(VERSION).tar
CLIENT_TAR=$(TARDIR)/$(PRODUCT)_client_$(VERSION).tar

CANNED_MODULES=pythreader jwt requests
CANNED_PIP_MODULES="pythreader>=2.8.0" "pyjwt" "requests"

all:	
	echo Use "make dune" or "make generic"

dune:
	make VERSION=`python metacat/version.py`_dune dune_with_version_defined

generic:
	make VERSION=`python metacat/version.py` generic_with_version_defined

dune_with_version_defined:	clean dune_specifics tars

generic_with_version_defined:	clean tars

tars:  build $(TARDIR)
	cd $(BUILD_DIR); tar cf $(SERVER_TAR) lib server daemon
	@echo \|
	@echo \| Server tarfile is created: $(SERVER_TAR)
	@echo \|

client:
	make VERSION=`python metacat/version.py` client_with_version_defined

client_with_version_defined: canned_client $(TARDIR)
	cd $(BUILD_DIR); tar cf $(CLIENT_TAR) lib dependencies ui canned_client_setup.sh
	@echo \|
	@echo \| Canned client tarfile is created: $(CLIENT_TAR)
	@echo \|

canned_client: build $(DEPS_DIR)
	pip install $(CANNED_PIP_MODULES)
	python tools/copy_modules.py -c $(CANNED_MODULES) $(DEPS_DIR)
	rm -rf $(DEPS_DIR)/*/__pycache__
	find $(DEPS_DIR) -type f -name \*.pyc -exec rm -f {} \;
	cp canned_client_setup.sh $(BUILD_DIR)

dune_specifics:
	cd DUNE_specials; make SERVER_DIR=$(SERVER_DIR) build

build:  $(BUILD_DIR) 
	cd src; make LIBDIR=$(LIBDIR) VERSION=$(VERSION) build
	cd daemon; make DAEMONDIR=$(DAEMONDIR) VERSION=$(VERSION) build
	cd metacat; make LIBDIR=$(LIBDIR) VERSION=$(VERSION) BINDIR=$(UI_DIR) build
	cd webserver; make SERVER_DIR=$(SERVER_DIR) LIBDIR=$(LIBDIR) VERSION=$(VERSION) build
	#cd docs; make SERVER_DIR=$(SERVER_DIR) DOCSDIR=$(DOCSDIR) -f Makefile-product build
	find $(BUILD_DIR) -type d -name __pycache__ -print | xargs rm -rf
	find $(BUILD_DIR) -type f -name \*.pyc -print -exec rm {} \;

clean:
	rm -rf $(BUILD_DIR) $(SERVER_TAR)
	
$(TARDIR):
	mkdir -p $@
	
$(DEPS_DIR):
	mkdir -p $@

$(BUILD_DIR):
	mkdir -p $@
	

	
