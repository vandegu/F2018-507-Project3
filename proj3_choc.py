import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'


# Create a new db:
def create_db():

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)

    conn.commit()

    # Create Countries table:
    statement =  '''
        CREATE TABLE 'Countries' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Alpha2' TEXT NOT NULL,
        'Alpha3' TEXT NOT NULL,
        'EnglishName' TEXT NOT NULL,
        'Region' TEXT NOT NULL,
        'Subregion' TEXT NOT NULL,
        'Population' INTEGER NOT NULL,
        'Area' REAL
        );
    '''
    cur.execute(statement)

    # Create Bars table:
    statement =  '''
        CREATE TABLE 'Bars' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Company' TEXT NOT NULL,
        'SpecificBeanBarName' TEXT,
        'REF' TEXT,
        'ReviewDate' TEXT,
        'CocoaPercent' REAL,
        'CompanyLocationId' INTEGER,
        'Rating' REAL,
        'BeanType' REAL,
        'BroadBeanOriginId' INTEGER
        );
    '''
    cur.execute(statement)

    conn.commit()
    conn.close()
    print('\nSuccesfully created db...\n')

# Populate the db...
def populate_db():

    # Populate Countries first (Bars has connections w/Countries...):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # Open the json file, read the correct info, and save to db.
    f = open('countries.json','r')
    jsond = json.loads(f.read())
    f.close()

    for i,c in enumerate(jsond):
        statement = '''
            INSERT INTO Countries (
            Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area
            ) VALUES (
            ?, ?, ?, ?, ?, ?, ?
            )
        '''

        insertions = (
        c['alpha2Code'], c['alpha3Code'], c['name'],c['region'],c['subregion'],c['population'],c['area']
        )

        #print(i)
        cur.execute(statement,insertions)
        conn.commit()

    print('\nSuccesfully populated Countries...\n')

    # Now to populate the Bars table.

    # Open and read from csv...
    with open('flavors_of_cacao_cleaned.csv','r') as f:
        csvreader = csv.reader(f)

        for i,r in enumerate(csvreader):
            if i == 0: # Skip header
                columns_to_be_populated = tuple(r)

            else:
                statement = '''
                    INSERT INTO Bars (
                    Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocationId,
                    Rating, BeanType, BroadBeanOriginId
                    ) VALUES (?, ?, ?, ?, ?, (
                    SELECT Id FROM Countries WHERE ?=EnglishName
                    ), ?, ?, (
                    SELECT Id FROM Countries WHERE ?=EnglishName
                    ));
                '''
                cur.execute(statement,tuple(r))
                conn.commit()

    print('\nSuccesfully populated Bars...must be St. Patty\'s day...\n')

create_db()
populate_db()


# # Part 2: Implement logic to process user commands
# def process_command(command):
#     return []
#
#
# def load_help_text():
#     with open('help.txt') as f:
#         return f.read()
#
# # Part 3: Implement interactive prompt. We've started for you!
# def interactive_prompt():
#     help_text = load_help_text()
#     response = ''
#     while response != 'exit':
#         response = input('Enter a command: ')
#
#         if response == 'help':
#             print(help_text)
#             continue
#
# # Make sure nothing runs or prints out when this file is run as a module
# if __name__=="__main__":
#     interactive_prompt()
