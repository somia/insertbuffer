PYTHON	:= python
ENV	:= $(PWD)/env

help::
	@ cat README

boot::
	git submodule update --init
	$(PYTHON) -m virtualenv $(ENV)
	$(ENV)/bin/easy_install MySQL-python
	cd signalfd && $(ENV)/bin/python setup.py install

install::
	mkdir -p $(ENV)/bin $(ENV)/lib/php5
	install -m 755 insertbufferd.py $(ENV)/bin/insertbufferd
	install -m 644 php/insertbuffer.php $(ENV)/lib/php5/

run::
	$(ENV)/bin/python insertbufferd.py db=test

test::
	cd php && php test.php

clean::
	rm -rf env/
	rm -f *.py[co]
