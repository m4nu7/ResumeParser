from docx2python import docx2python
import pandas as pd
import os
import re
import PyPDF2
from striprtf.striprtf import rtf_to_text
import logging as lg

lg.basicConfig(filename="resumeparser.log",level=lg.INFO, format= "%(asctime)s-%(name)s-%(levelname)s-%(message)s")

# Create streamHandlers
console_log = lg.StreamHandler()
console_log.setLevel(lg.INFO)
format = lg.Formatter("%(asctime)s-%(name)s-%(levelname)s-%(message)s")
console_log.setFormatter(format)

# Create custom logger
lg.getLogger("").addHandler(console_log)
logger = lg.getLogger("resumeparser")

class resumeparser:

    def __init__(self):
        """
        Creates resumeparser object. Takes folder path as input from the user
        """
        while True:
            try:
                self.__row_indexes = []
                self.__linkedin_links = {}
                self.__emailids = {}
                self.__github_ids = {}
                self.__resume_skills = {}
                self.folderPath = input("Please input the folder path : ")

                if not os.path.isdir(self.folderPath):
                    logger.error("Please enter a valid folder path : ")
                    continue
                else:
                    break

            except Exception as e:
                logger.error(str(e))
                continue

    def read_doc(self):
        """
        Read files with .docx extension. Appends regex and skills matched from the .docx file to the respective class variables

        """
        try:
            logger.info("=========================== read_doc function START =====================================")
            self.docfiles_lst = []
            # self.__docfilecontent_lst = []

            for fn in os.listdir(self.folderPath):
                if fn.endswith(".docx") or fn.endswith(".doc"):
                    self.docfiles_lst.append(fn)

            else:
                for fn in self.docfiles_lst:
                    # doc = docx.Document(self.folderPath + "\\" + fn)
                    self.__row_indexes.append(fn)

                    # Renaming .doc to .docx to avoid bad zip file error
                    if fn.endswith(".doc"):
                        ind = self.docfiles_lst.index(fn)
                        self.docfiles_lst[ind] = fn[0:fn.find(".doc")] + ".docx"
                        os.rename(self.folderPath + "\\" + fn, self.folderPath + "\\" + fn[0:fn.find(".doc")] + ".docx")
                        logger.info(fn + " reanamed to " + fn + ".docx to avoid bad zip file error ")
                logger.info("Created row indexes for the dataframe - for .docx files ")
                # print(self.docfiles_lst)

                for fn in self.docfiles_lst:
                    self.__docfilecontent_lst = []
                    linkedinfn = []
                    emailsfn = []
                    gitfn = []
                    doc_result = docx2python(self.folderPath + "\\" + fn)
                    # length = len(doc_result.body)
                    # print(doc_result)
                    logger.info(
                        "=========================== __extractStr function START =====================================")
                    self.__extractStr(doc_result.body)
                    self.__extractStr(doc_result.header)  # including header
                    self.__extractStr(doc_result.footer)  # including footer

                    doc_result.close()
                    # print(self.__docfilecontent_lst)
                    logger.info("Extracted the content from the body components, headers and footers of " +fn + " file ")

                    for s in self.__docfilecontent_lst:
                        for ss in s.split():
                            if self.__regex_email(ss):
                                emailsfn.append(self.__regex_email(ss))

                            if self.__regex_linkedin(ss):
                                linkedinfn.append(self.__regex_linkedin(ss))

                            if self.__regex_git(ss):
                                gitfn.append(self.__regex_git(ss))
                    logger.info("Extracted the linkedin, email and github info from " + fn + " file - if available")
                    #print(linkedinfn)

                    # skills matching
                    resume_skills = []
                    for s in self.__docfilecontent_lst:
                        if self.skillsMatching(s.split()) != []:
                            resume_skills.extend(self.skillsMatching(s.split()))
                    logger.info("Extracted skills matched info from " + fn + " file - if available")
                    #print(resume_skills)
                    # add to linked_links, emailids, github_ids dict with respective files as key

                    self.__emailids[fn] = list(set(emailsfn))
                    self.__github_ids[fn] = list(set(gitfn))
                    self.__linkedin_links[fn] = list(set(linkedinfn))
                    self.__resume_skills[fn] = list(set(resume_skills))

                # print(self.__docfilecontent_lst)
                # print(len(self.docfiles_lst))


        except Exception as e:
            logger.error(str(e))

    def __extractStr(self, lst):
        """
        Function to extract non-empty strings from the document data

        :param lst: list of contents in a document
        """
        try:
            if type(lst) == list:
                for ele in lst:
                    if type(ele) == str and ele != "":
                        self.__docfilecontent_lst.append(ele)
                    else:
                        self.__extractStr(ele)
            if type(lst) == str and lst != "":
                self.__docfilecontent_lst.append(lst)
            # return self.__docfilecontent_lst        # commented this to avoid duplicaation of data in __docfilecontent_lst
            # from doc.body, doc.header and doc.footer

        except Exception as e:
            logger.info(str(e))

    def __regex_linkedin(self, str_):
        """
        Matches a string object passed with regex pattern for linkedin ids

        :param str_: string object
        :return: str_ if matched
        """
        try:
            if re.search("(http(s)?:(\/\/))?([\w]+\.)?linkedin\.com\/(pub|in|profile)", str_) is not None:
                return str_
        except Exception as e:
            logger.error("Exception in regex_linkedin function : " + str(e))

    def __regex_email(self, str_):
        """
        Matches a string object passed with regex pattern for email ids

        :param str_: string object
        :return: str_ if matched
        """
        try:
            if re.search('[a-z0-9]+[\._]?[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}', str_) is not None:
                return str_
        except Exception as e:
            logger.error("Exception in regex_email function : " + str(e))

    def __regex_git(self, str_):
        """
        Matches a string object passed with regex pattern for github ids

        :param str_: string object
        :return: str_ if matched
        """
        try:
            if re.search("(http(s)?:(\/\/))?([\w]+\.)?github\.com\/", str_) is not None:
                return str_
        except Exception as e:
            logger.error("Exception in regex_git function : " + str(e))

    def build_dataframe(self):
        """
        Creates a dataframe with resume filenames as indexes and inserts columns=["email", "linkedin", "github", "skills"]
        and inserts data to the dataframe
        Saves dataframe into an csv file ExtractedResumes_data.csv
        """
        try :
            # Merge linkedin data, email and github data in one list for each resume
            row_Data_dict = {}
            logger.info("=========================== build_dataframe function START =====================================")
            for fn in self.__emailids:
                row_Data_dict[fn] = self.__emailids[fn]              # create row_Data_dict key: values from emails dict

            for fn in self.__linkedin_links:
                if fn in row_Data_dict:
                    row_Data_dict[fn].extend(self.__linkedin_links[fn])      # extend if key in row_Data_dict
                else:
                    row_Data_dict[fn] = self.__linkedin_links[fn]             # create if key not in row_Data_dict

            for fn in self.__github_ids:
                if fn in row_Data_dict:
                    row_Data_dict[fn].extend(self.__github_ids[fn])           # extend if key in row_Data_dict
                else:
                    row_Data_dict[fn] = self.__github_ids[fn]                 # create if key not in row_Data_dict

            for fn in self.__resume_skills:
                if fn in row_Data_dict:
                    if self.__resume_skills[fn] != []:
                        row_Data_dict[fn].append(self.__resume_skills[fn])    # extend if key in row_Data_dict
                else:
                    if self.__resume_skills[fn] != []:
                        row_Data_dict[fn] = [self.__resume_skills[fn] for i in range(1)] # create if key not in row_Data_dict

            logger.info("Building dataframe for : " + str(row_Data_dict))

            # Create dataframe with NaN values
            df = pd.DataFrame(index=self.__row_indexes, columns=["email", "linkedin", "github", "skills"])

            # Insert into dataframe
            for fn in row_Data_dict:
                if row_Data_dict[fn] != []:
                    for s in row_Data_dict[fn]:
                        if "linkedin." in s:
                            df["linkedin"][fn] = s
                        elif "github." in s:
                            df["github"][fn] = s
                        elif type(s) == list:
                            df["skills"][fn] = s
                        else:
                            df["email"][fn] = s
            df.to_csv("ExtractedResumes_data.csv")
            logger.info("DataFrame created and inserted values. Please check ExtractedResumes_data.csv")
        except Exception as e :
            logger.error("ERROR in build_dataframe function " + str(e))

    def skillsMatching(self, lst):
        """
        Matched skills listed in a resume file with skills in skills.txt file

        :param lst:  list of file contents/text
        :return: skills_list, list which contains list of all matched skills from skills.txt file
        """
        try :
            with open("skills.txt", "r") as f:
                content = f.read().split("\n")

            content = [ele.upper() for ele in content]

            skills_lst = []
            for ele in lst:
                if ele.upper() in content:
                    skills_lst.append(ele.upper())

            return skills_lst
        except Exception as e :
            logger.error("ERROR in skillsMatching function : " + str(e))

    def read_pdf(self):
        """
        Read files with .pdf extension. Appends regex and skills matched from the .pdf file to the respective class variables

        """
        logger.info("=========================== read_pdf function START =====================================")
        try:
            self.pdffiles_lst = []

            for fn in os.listdir(self.folderPath):
                if fn.endswith(".pdf"):
                    self.pdffiles_lst.append(fn)

            else:
                logger.info("Created row indexes for the dataframe - for .pdf files ")
                for fn in self.pdffiles_lst:
                    self.__row_indexes.append(fn)

                #print(self.pdffiles_lst)

                for fn in self.pdffiles_lst:
                    self.__pdffilecontent_lst = []
                    linkedinfn = []
                    emailsfn = []
                    gitfn = []

                    # create a pdffile object
                    pdfFileobj = open(self.folderPath + "\\" + fn, "rb")

                    # create a pdf reader object
                    pdfReader = PyPDF2.PdfReader(pdfFileobj)

                    # extract text from pages
                    for i in range(len(pdfReader.pages)):
                        pageObj = pdfReader.pages[i]
                        self.__pdffilecontent_lst.extend(pageObj.extract_text().split("\n"))

                    pdfFileobj.close()
                    logger.info("Extracted the content from " + fn + " file ")

                    for s in self.__pdffilecontent_lst:
                        for ss in s.split():
                            if self.__regex_email(ss):
                                emailsfn.append(self.__regex_email(ss))

                            if self.__regex_linkedin(ss):
                                linkedinfn.append(self.__regex_linkedin(ss))

                            if self.__regex_git(ss):
                                gitfn.append(self.__regex_git(ss))

                    logger.info("Extracted the linkedin, email and github info from " + fn + " file - if available")

                    #print(linkedinfn)

                    # skills matching
                    resume_skills = []
                    for s in self.__pdffilecontent_lst:
                        if self.skillsMatching(s.split()) != []:
                            resume_skills.extend(self.skillsMatching(s.split()))
                    logger.info("Extracted skills matched info from " + fn + " file - if available")

                    # add to linked_links, emailids, github_ids dict with respective files as key

                    self.__emailids[fn] = list(set(emailsfn))
                    self.__github_ids[fn] = list(set(gitfn))
                    self.__linkedin_links[fn] = list(set(linkedinfn))
                    self.__resume_skills[fn] = list(set(resume_skills))

                # print(self.__docfilecontent_lst)
                # print(len(self.docfiles_lst))

        except Exception as e :
            logger.error("ERROR in read_pdf function : " +str(e))

    def read_rtf(self):
        """
        Read files with .rtf extension. Appends regex and skills matched from the .rtf file to the respective class variables
        """
        logger.info("=========================== read_rtf function START =====================================")
        try:
            self.rtffiles_lst = []

            for fn in os.listdir(self.folderPath):
                if fn.endswith(".rtf"):
                    self.rtffiles_lst.append(fn)

            else:
                logger.info("Created row indexes for the dataframe - for .rtf files ")
                for fn in self.rtffiles_lst:
                    self.__row_indexes.append(fn)

                #print(self.rtffiles_lst)

                for fn in self.rtffiles_lst:
                    self.__rtffilecontent_lst = []
                    linkedinfn = []
                    emailsfn = []
                    gitfn = []

                    # create a rtfffile object
                    rtfFileobj = open(self.folderPath + "\\" + fn, "r")
                    content = rtfFileobj.read()
                    text = rtf_to_text(content)
                    self.__rtffilecontent_lst.extend(text.split())

                    rtfFileobj.close()
                    logger.info("Extracted the content from " + fn + " file ")

                    for ss in self.__rtffilecontent_lst:
                        if self.__regex_email(ss):
                            emailsfn.append(self.__regex_email(ss))

                        if self.__regex_linkedin(ss):
                            linkedinfn.append(self.__regex_linkedin(ss))

                        if self.__regex_git(ss):
                            gitfn.append(self.__regex_git(ss))

                    logger.info("Extracted the linkedin, email and github info from " + fn + " file - if available")

                    #print(self.__rtffilecontent_lst)

                    # skills matching
                    resume_skills = []

                    if self.skillsMatching(self.__rtffilecontent_lst) != []:
                        resume_skills.extend(self.skillsMatching(self.__rtffilecontent_lst))
                    logger.info("Extracted skills matched info from " + fn + " file - if available")

                    # add to linked_links, emailids, github_ids dict with respective files as key
                    self.__emailids[fn] = list(set(emailsfn))
                    self.__github_ids[fn] = list(set(gitfn))
                    self.__linkedin_links[fn] = list(set(linkedinfn))
                    self.__resume_skills[fn] = list(set(resume_skills))


        except Exception as e:
            logger.error("ERROR in read_rtf function : ", str(e))