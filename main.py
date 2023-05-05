from resumeparserMod import resumeparser

def app() :
    p = resumeparser()
    p.read_doc()
    p.read_pdf()
    p.read_rtf()
    p.build_dataframe()

if __name__ == "__main__" :
    app()