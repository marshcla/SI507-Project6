# Import statements
import psycopg2
import psycopg2.extras
from config import *
import csv


# Write code / functions to set up database connection and cursor here.
db_connection, db_cursor = None, None

def get_connection_and_cursor():
    global db_connection, db_cursor
    if not db_connection:
        try:
            if db_password != "":
                db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
                print("Success connecting to database")
            else:
                db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
        except:
            print("Unable to connect to the database. Check server and credentials.")
            sys.exit(1) # Stop running program if there's no db connection.

    if not db_cursor:
        db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

db_connection, db_cursor = get_connection_and_cursor()
print(db_connection, db_cursor)

def execute_and_print(query, numer_of_results=1):
    cur.execute(query)
    results = cur.fetchall()
    for r in results[:numer_of_results]:
        print(r)
    print('--> Result Rows:', len(results))
    print()

# Write code / functions to deal with CSV files and insert data into the database here.
arCSVINSERT = open("arkansas.csv", 'r')
reader_a = csv.DictReader(arCSVINSERT)
db_cursor.execute("""INSERT INTO states ("name") VALUES (%s) ON CONFLICT DO NOTHING RETURNING id""", ('Arkansas',) );
result = db_cursor.fetchall()
state_id = result[0]['id']
for row in reader_a:
    db_cursor.execute("""INSERT INTO sites ("name", "type", "location", "description", "state_id") VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""", (row['NAME'], row['TYPE'], row['LOCATION'], row['DESCRIPTION'], state_id));
db_connection.commit()

### HAVING ENCODING ISSUES THAT MAKE IT IMPOSSIBLE TO INSERT CALIFORNIA CSV INTO DATABASE ### 

# caCSVINSERT2 = open("california.csv", 'r', encoding='utf-8')
# reader_c = csv.DictReader(caCSVINSERT2)
# db_cursor.execute("""INSERT INTO states ("name") VALUES (%s) ON CONFLICT DO NOTHING RETURNING id""", ('California',) );
# result2 = db_cursor.fetchall()
# state_id2 = result2[0]['id']
# for row in reader_c:
#     db_cursor.execute("""INSERT INTO sites ("name", "type", "location", "description", "state_id") VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""", (row['NAME'], row['TYPE'], row['LOCATION'], row['DESCRIPTION'], state_id2));
#     state_id = db_cursor.fetchall()[0][0]
# db_connection.commit()

miCSVINSERT2 = open("michigan.csv", 'r')
reader_m = csv.DictReader(miCSVINSERT2)
db_cursor.execute("""INSERT INTO states ("name") VALUES (%s) ON CONFLICT DO NOTHING RETURNING id""", ('Michigan',) );
result3 = db_cursor.fetchall()
state_id3 = result3[0]['id']
for row in reader_m:
    db_cursor.execute("""INSERT INTO sites ("name", "type", "location", "description", "state_id") VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""", (row['NAME'], row['TYPE'], row['LOCATION'], row['DESCRIPTION'], state_id3));
db_connection.commit()


# Make sure to commit your database changes with .commit() on the database connection.
# Write code to be invoked here (e.g. invoking any functions you wrote above)
### Check the README. ###

# Write code to make queries and save data in variables here.

def execute_and_print(query, numer_of_results=15):
    db_cursor.execute(query)
    results = db_cursor.fetchall()
    for r in results[:numer_of_results]:
        print(r)
    print('--> Result Rows:', len(results))
    print()

all_locations = execute_and_print('SELECT "location" FROM "sites"')

beautiful_sites = execute_and_print("""SELECT "name" FROM "sites" WHERE "description" LIKE '%beautiful%'""")
# Because I couldn't enter the California data into the database, I didn't have any sites with the word 'beautiful' in their descriptions.

test_description = execute_and_print("""SELECT "name" FROM "sites" WHERE "description" LIKE '%water%'""")
# Made a test to see if I could pull out sites with 'water' in their descriptions instead.

natl_lakeshores = execute_and_print("""SELECT "name" FROM "sites" WHERE "type" = 'National Lakeshore'""")

michigan_names = execute_and_print("""SELECT "sites"."name" FROM "sites" INNER JOIN "states" ON ("states"."id" = "sites"."state_id") WHERE "states"."name" = 'Michigan' """)

total_number_arkansas = execute_and_print("""SELECT COUNT("sites"."name") FROM "sites" INNER JOIN "states" ON ("states"."id" = "sites"."state_id") WHERE "states"."name" = 'Arkansas' """)




# We have not provided any tests, but you could write your own in this file or another file, if you want.
