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
                continue

            else:
                r[4] = float(r[4].strip('%'))/100
                #print(r)
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

# create_db()
# populate_db()

# Part 2: Implement logic to process user commands
def process_command(command):

    # First, split the command into individual commands.
    cl = command.split()

    # Start logic commands with outermost commands:
    if 'bars' in cl:

        # Define the default commands:
        sql_params = dict(
        select =
        '''
            SELECT b.SpecificBeanBarName,b.Company,cid.EnglishName,b.Rating,b.CocoaPercent,bid.EnglishName
            FROM Bars AS b
            JOIN Countries AS cid ON b.CompanyLocationId=cid.Id
            LEFT JOIN Countries AS bid ON b.BroadBeanOriginId=bid.Id
        ''',
        where = "WHERE 1=1",
        sortby = "ORDER BY b.Rating",
        limit = "DESC LIMIT 10"
        )
        # Parse through each command:
        for comm in cl:
            # Split the individual command again to account for determined variables.
            c = comm.split('=')
            if c[0] == 'ratings':
                sql_params['sortby'] = 'ORDER BY b.Rating'
            elif c[0] == 'cocoa':
                sql_params['sortby'] = 'ORDER BY b.CocoaPercent'
            elif c[0] == 'sellcountry':
                sql_params['where'] = '''
                    JOIN Countries AS c ON b.CompanyLocationId=c.Id
                    WHERE c.Alpha2=\'{}\'
                '''.format(c[1])
            elif c[0] == 'sellregion':
                sql_params['where'] = '''
                    JOIN Countries AS c ON b.CompanyLocationId=c.Id
                    WHERE c.Region=\'{}\'
                '''.format(c[1])
            elif c[0] == 'sourcecountry':
                sql_params['where'] = '''
                    JOIN Countries AS c ON b.BroadBeanOriginId=c.Id
                    WHERE c.Alpha2=\'{}\'
                '''.format(c[1])
            elif c[0] == 'sourceregion':
                sql_params['where'] = '''
                    JOIN Countries AS c ON b.BroadBeanOriginId=c.Id
                    WHERE c.Region=\'{}\'
                '''.format(c[1])
            elif c[0] == 'top':
                sql_params['limit'] = 'DESC LIMIT {}'.format(c[1])
            elif c[0] == 'bottom':
                sql_params['limit'] = 'ASC LIMIT {}'.format(c[1])

        # Create the SQL statement:
        statement = sql_params['select']+' '+sql_params['where']+' '+sql_params['sortby']+' '+sql_params['limit']
        # Connect and execute:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        #print(statement)
        cur.execute(statement)

        out = []
        for r in cur:
            out.append(r)

        conn.close()

        return out

    elif 'companies' in cl:

        # Define the default commands:
        sql_params = dict(
        select =
        '''
            SELECT b.Company, cid.EnglishName, AVG(b.Rating) FROM Bars AS b
            JOIN Countries AS cid ON b.CompanyLocationId=cid.Id
            GROUP BY b.Company
        ''',
        where = "HAVING COUNT(b.SpecificBeanBarName)>4",
        sortby = "ORDER BY AVG(b.Rating)",
        limit = "DESC LIMIT 10"
        )

        for comm in cl:
            # Split the individual command again to account for determined variables.
            c = comm.split('=')
            if c[0] == 'ratings':
                continue # Already the default.
            elif c[0] == 'cocoa':
                sql_params['select'] = '''
                    SELECT b.Company, cid.EnglishName, AVG(b.CocoaPercent) FROM Bars AS b
                    JOIN Countries AS cid ON b.CompanyLocationId=cid.Id
                    GROUP BY b.Company
                '''
                sql_params['sortby'] = "ORDER BY AVG(b.CocoaPercent)"
            elif c[0] == 'bars_sold':
                sql_params['select'] = '''
                    SELECT b.Company, cid.EnglishName, COUNT(b.SpecificBeanBarName) FROM Bars AS b
                    JOIN Countries AS cid ON b.CompanyLocationId=cid.Id
                    GROUP BY b.Company
                '''
                sql_params['sortby'] = "ORDER BY COUNT(b.SpecificBeanBarName)"
            elif c[0] == 'country':
                sql_params['where'] += ' AND cid.Alpha2=\'{}\''.format(c[1])
            elif c[0] == 'region':
                sql_params['where'] += ' AND cid.Region=\'{}\''.format(c[1])
            elif c[0] == 'top':
                sql_params['limit'] = 'DESC LIMIT {}'.format(c[1])
            elif c[0] == 'bottom':
                sql_params['limit'] = 'ASC LIMIT {}'.format(c[1])

        # Create the SQL statement:
        statement = sql_params['select']+' '+sql_params['where']+' '+sql_params['sortby']+' '+sql_params['limit']
        # Connect and execute:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        #print('\n',statement)
        cur.execute(statement)

        out = []
        for r in cur:
            out.append(r)

        conn.close()

        return out

    elif 'countries' in cl:

        # Define the default commands:
        sql_params = dict(
        select =
        '''
            SELECT c.EnglishName, c.Region, AVG(b.Rating) FROM Bars AS b
        ''',
        join =
        '''
            JOIN Countries AS c ON b.CompanyLocationId=c.Id GROUP BY c.EnglishName
        ''',
        where = "HAVING COUNT(*)>4",
        sortby = "ORDER BY AVG(b.Rating)",
        limit = "DESC LIMIT 10"
        )

        for comm in cl:
            # Split the individual command again to account for determined variables.
            c = comm.split('=')
            if c[0] == 'ratings':
                continue # default
            elif c[0] == 'cocoa':
                sql_params['select'] = '''
                    SELECT c.EnglishName, c.Region, AVG(b.Rating) FROM Bars AS b
                '''
                sql_params['sortby'] = "ORDER BY AVG(b.CocoaPercent)"
            elif c[0] == 'bars_sold':
                sql_params['select'] = '''
                    SELECT c.EnglishName, c.Region, COUNT(*) FROM Bars AS b
                '''
                sql_params['sortby'] = "ORDER BY COUNT(*)"
            elif c[0] == 'sellers':
                continue # default
            elif c[0] == 'sources':
                sql_params['join'] = '''
                    JOIN Countries AS c ON b.BroadBeanOriginId=c.Id GROUP BY c.EnglishName
                '''
            elif c[0] == 'region':
                sql_params['where'] += ' AND cid.Region=\'{}\''.format(c[1])
            elif c[0] == 'top':
                sql_params['limit'] = 'DESC LIMIT {}'.format(c[1])
            elif c[0] == 'bottom':
                sql_params['limit'] = 'ASC LIMIT {}'.format(c[1])

        # Create the SQL statement:
        statement = sql_params['select']+' '+sql_params['join']+' '+sql_params['where']+' '+sql_params['sortby']+' '+sql_params['limit']
        # Connect and execute:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        #print('\n',statement)
        cur.execute(statement)

        out = []
        for r in cur:
            out.append(r)

        conn.close()

        return out

    elif 'regions' in cl:

        # Define the default commands:
        sql_params = dict(
        select =
        '''
            SELECT c.Region, AVG(b.Rating) FROM Bars AS b
        ''',
        join =
        '''
            JOIN Countries AS c ON b.CompanyLocationId=c.Id GROUP BY c.Region
        ''',
        where = "HAVING COUNT(*)>4",
        sortby = "ORDER BY AVG(b.Rating)",
        limit = "DESC LIMIT 10"
        )

        for comm in cl:
            # Split the individual command again to account for determined variables.
            c = comm.split('=')
            if c[0] == 'ratings':
                continue # default
            elif c[0] == 'cocoa':
                sql_params['select'] = '''
                    SELECT c.Region, AVG(b.Rating) FROM Bars AS b
                '''
                sql_params['sortby'] = "ORDER BY AVG(b.CocoaPercent)"
            elif c[0] == 'bars_sold':
                sql_params['select'] = '''
                    SELECT c.Region, COUNT(*) FROM Bars AS b
                '''
                sql_params['sortby'] = "ORDER BY COUNT(*)"
            elif c[0] == 'sellers':
                continue # default
            elif c[0] == 'sources':
                sql_params['join'] = '''
                    JOIN Countries AS c ON b.BroadBeanOriginId=c.Id GROUP BY c.Region
                '''
            elif c[0] == 'top':
                sql_params['limit'] = 'DESC LIMIT {}'.format(c[1])
            elif c[0] == 'bottom':
                sql_params['limit'] = 'ASC LIMIT {}'.format(c[1])

        # Create the SQL statement:
        statement = sql_params['select']+' '+sql_params['join']+' '+sql_params['where']+' '+sql_params['sortby']+' '+sql_params['limit']
        # Connect and execute:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
        #print('\n',statement)
        cur.execute(statement)

        out = []
        for r in cur:
            out.append(r)

        conn.close()

        return out















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
