#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: This program provides functions to interact with a PostgreSQL database for document and category management.
# It includes functionalities to create, delete, update documents, and retrieve term-document associations.
# FOR: CS 4250- Assignment #2
# TIME SPENT: 3h
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import psycopg2
from psycopg2.extras import RealDictCursor
import re
import string

def connectDataBase():

    # Create a database connection object using psycopg2
    # --> add your Python code here
    DB_NAME = "corpus"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        return conn
    except:
        print("Database not connected successfully")

def createCategory(cur, catId, catName):
    # Insert a category in the database
    # --> add your Python code here
    sql = "Insert into category (cat_id, name) Values (%s, %s)"
    recset = [catId, catName]
    cur.execute(sql, recset)
    print("Category created.")
def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    # --> add your Python code here
    cur.execute("SELECT cat_id FROM category WHERE name = %s", (docCat,))
    result = cur.fetchone()
    if result is not None:
        catId = result['cat_id']
    else:
        print(f"Category '{docCat}' does not exist.")
        return
    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # --> add your Python code here
    docText_cleaned = re.sub(r'[^\w]', '', docText)
    numChar = len(docText_cleaned)
    sql = "Insert into document(doc_id, text, title, num_chars, date, cat_id) Values (%s, %s, %s, %s, %s, %s)"
    recset = [docId, docText, docTitle, numChar, docDate, catId]
    cur.execute(sql, recset)
    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    # --> add your Python code here
    indexList = {}
    termList = {}
    newTermList = {}
    # Split docText into terms, remove punctuation
    translator = str.maketrans('', '', string.punctuation)
    termInDoc = [term.translate(translator).lower() for term in docText.split() if term]

    # Retrieve all terms
    cur.execute("SELECT * FROM term")
    result = cur.fetchall()
    for row in result:
        term = row['term']
        num_chars = row['num_chars']
        termList[term] = num_chars
        newTermList[term] = num_chars

    # Update indexList
    for term in termInDoc:
        if term not in termList:
            newTermList[term] = len(term)

    # Update term database
    for term, num_chars in newTermList.items():
        if term not in termList:
            sql = "Insert into term(term, num_chars) Values (%s, %s)"
            recset = [term, num_chars]
            cur.execute(sql, recset)

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    # --> add your Python code here

    # Update indexList
    for term in termInDoc:
        if term in indexList:
            indexList[term] += 1
        else:
            indexList[term] = 1

    # Update index database
    for term, count in indexList.items():
        sql = "Insert into index(doc_id, term, count) Values (%s, %s, %s)"
        recset = [docId, term, count]
        cur.execute(sql, recset)
    print("Document created.")

def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here
    # Retrieve all terms
    cur.execute("SELECT term FROM index WHERE doc_id = %s", (docId,))
    termInDoc = cur.fetchall()
    for row in termInDoc:
        term = row['term']
        sql = "Delete from index where doc_id = %(doc_id)s AND term = %(term)s"
        cur.execute(sql, {'doc_id': docId, 'term': term})
        cur.execute("SELECT term FROM index WHERE term = %s", (term,))
        termInOtherDoc = cur.fetchall()
        if len(termInOtherDoc) == 0:
            sql = "Delete from term where term = %(term)s"
            cur.execute(sql, {'term': term})

    # 2 Delete the document from the database
    # --> add your Python code here
    sql = "Delete from document where doc_id = %(doc_id)s"
    cur.execute(sql, {'doc_id': docId})
    print("Document deleted.")

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    # --> add your Python code here
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    # --> add your Python code here
    createDocument(cur, docId, docText, docTitle, docDate, docCat)
    print("Document updated.")
def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    termList = {}

    # Retrieve all terms
    cur.execute("SELECT term FROM term ORDER BY term ASC")
    terms = cur.fetchall()

    for term_row in terms:
        term = term_row['term']

        # Retrieve documents where the term occurs with their counts
        cur.execute("""
                SELECT d.title, SUM(i.count) as count
                FROM document d
                JOIN index i ON d.doc_id = i.doc_id
                WHERE i.term = %s
                GROUP BY d.title
            """, (term,))
        term_documents = cur.fetchall()

        # Create a dictionary of document counts for the term
        docCountItems = [f"{row['title']}:{row['count']}" for row in term_documents]
        docCount = ", ".join(docCountItems)
        termList[term] = docCount

    return termList