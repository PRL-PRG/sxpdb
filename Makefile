R=R
Rscript=Rscript

.PHONY: all build check document test write trace

all: document build check

build: document
	$R CMD build .

check: build
	$R CMD check record*tar.gz

clean:
	-rm -f sxpdb*tar.gz
	-rm -fr sxpdb.Rcheck
	-rm -rf src/*.o src/*.so
	-rm -rf tests/testthat/test_db*
	-rm -f trace

document:
	$Rscript -e 'devtools::document()'

test:
	$Rscript -e 'devtools::test()'

clean_test:
	-rm -rf tests/testthat/test_db*
	-rm -rf tests/testthat/_snaps

trace:
	strace Rscript -e 'devtools::test()' 2> trace

lintr:
	$R --slave -e "lintr::lint_package()"

install: clean
	$R CMD INSTALL .

write:
	cat trace | grep -w "write"
