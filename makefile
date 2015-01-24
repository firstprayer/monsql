
test:
	python test.py

reqs:
	pip install -r requirements.txt

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

.PHONY: test reqs install register upload clean
