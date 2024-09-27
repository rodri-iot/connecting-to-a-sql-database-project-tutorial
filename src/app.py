import os
from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
from dotenv import load_dotenv
import sqlite3
# load the .env file variables
load_dotenv()

# 1) Connect to the database here using the SQLAlchemy's create_engine function

# Create the connection string
def connect():
    global engine

    connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    print("Starting the connection...")
    engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
    engine.connect()
    return engine

# Apply
engine = connect()
print(engine)

# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function

def create_tables(engine):
    # CCreate table "publishers"
    query = text("""
        CREATE TABLE IF NOT EXISTS publishers (
            publisher_id INT NOT NULL,
            name VARCHAR(255) NOT NULL,
            PRIMARY KEY(publisher_id)
        );
    """)
    engine.execute(query)

    # Create table "authors"
    query = text("""
        CREATE TABLE IF NOT EXISTS authors (
            author_id INT NOT NULL,
            first_name VARCHAR(100) NOT NULL,
            middle_name VARCHAR(50) NULL,
            last_name VARCHAR(100) NULL,
            PRIMARY KEY(author_id)
        );
    """)
    engine.execute(query)

    # Create table "books"
    query = text("""
        CREATE TABLE IF NOT EXISTS books (
            book_id INT NOT NULL,
            title VARCHAR(255) NOT NULL,
            total_pages INT NULL,
            rating DECIMAL(4, 2) NULL,
            isbn VARCHAR(13) NULL,
            published_date DATE,
            publisher_id INT NULL,
            PRIMARY KEY(book_id),
            CONSTRAINT fk_publisher FOREIGN KEY(publisher_id) REFERENCES publishers(publisher_id)
        );
    """)
    engine.execute(query)

    # Create table "book_authors"
    query = text("""
        CREATE TABLE IF NOT EXISTS book_authors (
            book_id INT NOT NULL,
            author_id INT NOT NULL,
            PRIMARY KEY(book_id, author_id),
            CONSTRAINT fk_book FOREIGN KEY(book_id) REFERENCES books(book_id) ON DELETE CASCADE,
            CONSTRAINT fk_author FOREIGN KEY(author_id) REFERENCES authors(author_id) ON DELETE CASCADE
        );
    """)
    engine.execute(query)

# USE
engine = connect()
create_tables(engine)

# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function

def insert_data(engine):
    # Insert data into table "publishers"
    query = text("""
        INSERT INTO publishers(publisher_id, name)
        VALUES (1, 'O Reilly Media'),
               (2, 'A Book Apart'),
               (3, 'A K PETERS'),
               (4, 'Academic Press'),
               (5, 'Addison Wesley'),
               (6, 'Albert&Sweigart'),
               (7, 'Alfred A. Knopf');
    """)
    engine.execute(query)

    # Insert data into table "authors"
    query = text("""
        INSERT INTO authors (author_id, first_name, middle_name, last_name)
        VALUES (1, 'Merritt', null, 'Eric'),
               (2, 'Linda', null, 'Mui'),
               (3, 'Alecos', null, 'Papadatos'),
               (4, 'Anthony', null, 'Molinaro'),
               (5, 'David', null, 'Cronin'),
               (6, 'Richard', null, 'Blum'),
               (7, 'Yuval', 'Noah', 'Harari'),
               (8, 'Paul', null, 'Albitz');
    """)
    engine.execute(query)

    # Insert data into table "books"
    query = text("""
        INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id)
        VALUES (1, 'Lean Software Development: An Agile Toolkit', 240, 4.17, '9780320000000', '2003-05-18', 5),
               (2, 'Facing the Intelligence Explosion', 91, 3.87, null, '2013-02-01', 7),
               (3, 'Scala in Action', 419, 3.74, '9781940000000', '2013-04-10', 1),
               (4, 'Patterns of Software: Tales from the Software Community', 256, 3.84, '9780200000000', '1996-08-15', 1),
               (5, 'Anatomy Of LISP', 446, 4.43, '9780070000000', '1978-01-01', 3),
               (6, 'Computing machinery and intelligence', 24, 4.17, null, '2009-03-22', 4),
               (7, 'XML: Visual QuickStart Guide', 269, 3.66, '9780320000000', '2009-01-01', 5),
               (8, 'SQL Cookbook', 595, 3.95, '9780600000000', '2005-12-01', 7),
               (9, 'The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)', 439, 4.29, '9781440000000', '2010-07-01', 6),
               (10, 'Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence', 222, 3.54, '9780750000000', '2007-02-13', 7);
    """)
    engine.execute(query)

    # Insert data into table "book_authors"
    query = text("""
        INSERT INTO book_authors (book_id, author_id)
        VALUES (1, 1),
               (2, 8),
               (3, 7),
               (4, 6),
               (5, 5),
               (6, 4),
               (7, 3),
               (8, 2),
               (9, 4),
               (10, 1);
    """)
    engine.execute(query)

# Use
engine = connect()
insert_data(engine)

# 4) Use pandas to print one of the tables as dataframes using read_sql function

def print_books(engine):
    # Create a connection from the engine
    with engine.connect() as connection:
        df_books = pd.read_sql('SELECT * FROM books', connection.connection)
        print(df_books)

# Now use it
print_books(engine)