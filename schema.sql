-- Create a table to store category
CREATE TABLE category (
    cat_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

-- Create a table to store documents
CREATE TABLE document (
    doc_id INTEGER PRIMARY KEY,
    cat_id INTEGER NOT NULL,
    text TEXT NOT NULL,
    title TEXT NOT NULL,
    num_chars INTEGER NOT NULL,
    date TEXT NOT NULL,
    FOREIGN KEY (cat_id) REFERENCES category(cat_id)
);

-- Create a table to store term
CREATE TABLE term (
    term TEXT PRIMARY KEY,
    num_chars INTEGER NOT NULL
);
-- Create a table to store index
CREATE TABLE "index" (
    doc_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    count INTEGER NOT NULL,
    PRIMARY KEY (doc_id, term),
    FOREIGN KEY (doc_id) REFERENCES document(doc_id),
    FOREIGN KEY (term) REFERENCES "term"("term")
);

