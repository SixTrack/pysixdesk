
TEXFLAGS  = 
TEXFILES := $(wildcard *.tex)

all: pysixdesk.pdf

clean:
	rm -f pysixdesk.aux pysixdesk.fdb_latexmk pysixdesk.fls pysixdesk.idx pysixdesk.ilg pysixdesk.ind pysixdesk.log pysixdesk.lot pysixdesk.out pysixdesk.toc pysixdesk.pdf pysixdesk.synctex.gz chIntroduction.log incAcknowledgement.log incTitlePage.log

pysixdesk.pdf: $(TEXFILES)
	pdflatex $(TEXFLAGS) pysixdesk.tex
	pdflatex $(TEXFLAGS) pysixdesk.tex
