
test:
	python test.py

register:
	python setup.py register

upload:
	python setup.py sdist bdist_wininst upload
