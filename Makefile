
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
	echo Use "make dune" or "make generic"
	
dune:
	make VERSION=`python metacat/version.py`_dune dune_with_version_defined

generic:
	make VERSION=`python metacat/version.py` generic_with_version_defined

dune_with_version_defined:	build dune_specifics
	make VERSION=$(VERSION) tars
	
generic_with_version_defined:	build
	make VERSION=$(VERSION) tars

tars:  $(TARDIR)
	cd $(BUILD_DIR); tar cf $(SERVER_TAR) lib server docs
	@echo \|
	@echo \| tarfile is created: $(SERVER_TAR)
	@echo \|

dune_specifics:
	cd DUNE_specials; make SERVER_DIR=$(SERVER_DIR) build

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
	

	
