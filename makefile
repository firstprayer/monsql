
test:
	python test.py

doc: install
	cd doc; make clean; make html

install:
	python setup.py install

register:
	python setup.py register

upload:
	python setup.py sdist bdist_wininst upload

clean:
	rm monsql/*.pyc

.PHONY: test install register upload clean
