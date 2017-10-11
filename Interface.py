#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import multiprocessing
import numpy as np
import time

DATABASE_NAME = 'dds_assgn1'
lin = 0
part = 1

#have changed user and password here
def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def something(tname,fname,con):
	rows = []
	arr=[]
	f = open(fname, 'r')
	cur = con.cursor()
	cur.execute("DROP TABLE IF EXISTS "+tname+" CASCADE")
	#print "dropped"
	cur.execute("CREATE TABLE " + tname + " (UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating REAL, temp5 VARCHAR(10), Timestamp INT)")	
	#print "created"	
	#cur.execute("SELECT * FROM "+tname);
	cur.copy_from(f, tname, sep=':', columns=('UserID', 'temp1', 'MovieID', 'temp3', 'Rating', 'temp5', 'Timestamp'))
        cur.execute("ALTER TABLE " + tname + " DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")
	
	'''	
	cur.execute("SELECT * FROM "+tname);
	rows2 = cur.fetchall()
        for row2 in rows2:
		print row2
	'''
	cur.close()
	con.commit()
	

def loadratings(tname,fname,con):
	something(tname,fname,con)


def part_create_range(tname, con):
	cur = con.cursor()
	cur.execute("SELECT * FROM "+tname)
	rows = cur.fetchall()
	#print rows
	cur.execute("DELETE FROM "+tname)
        q = "INSERT INTO "+tname+" (userid, movieid, rating) VALUES (%s, %s, %s)"
	cur.executemany(q, rows)

	cur.execute("SELECT * FROM "+tname);
    	rows2 = cur.fetchall()
	for row2 in rows2:
	    print row2
		
	con.commit()
    
def rangepartition(tname, n, con):
    global part
    part = n
    ran = 5/n
    sumran = 0
    cur = con.cursor()
    table = []
   # cur.execute("DROP TABLE IF EXISTS "+tname+" CASCADE")
    for i in range(0,n):
        cur.execute("DROP TABLE IF EXISTS "+"range_part"+str(i))
   # may create constraint violation here
	if i == 0:
		cur.execute("CREATE TABLE "+"range_part"+str(i)+" (CHECK ( rating >= "+str(sumran)+" AND rating <= "+str(sumran+ran)+" )) INHERITS ("+tname+");");
	else:
		cur.execute("CREATE TABLE "+"range_part"+str(i)+" (CHECK ( rating > "+str(sumran)+" AND rating <= "+str(sumran+ran)+" )) INHERITS ("+tname+");");
	sumran=sumran+ran
	table.append("range_part"+str(i))
    #print "range"
    ran = 5/n
    sumran = 0
    q = "CREATE OR REPLACE FUNCTION ratings_insert_trigger() RETURNS TRIGGER AS \n $$ \n BEGIN \n"
    for i in range(n):
	if i == 0:
    		q = q+"IF ( NEW.rating >= "+str(sumran)+" AND NEW.rating <= "+str(sumran+ran)+") THEN INSERT INTO "+table[i]+" VALUES (NEW.*);\n ELSE"
	else:
		q = q+"IF ( NEW.rating >= "+str(sumran)+" AND NEW.rating <= "+str(sumran+ran)+") THEN INSERT INTO "+table[i]+" VALUES (NEW.*);\n ELSE"
    	sumran=sumran+ran
    #q = q+"IF ( NEW.rating = "+str(sumran)+") THEN INSERT INTO "+table[i]+" VALUES (NEW.*);\n ELSE"
    q = q+"\nRAISE EXCEPTION 'Rating out of range. Fix the measurement_insert_trigger() function!';\n END IF;\n RETURN NULL;\nEND;\n$$\nLANGUAGE plpgsql;"
    print q
    cur.execute(q)
    tri = "CREATE TRIGGER range_insert_trigger BEFORE INSERT ON "+tname+" FOR EACH ROW EXECUTE PROCEDURE ratings_insert_trigger();"
    cur.execute(tri)
    
    part_create_range(tname, con)
    
    con.commit()


def part_create_rr(tname,n,con):
	global lin
	cur = con.cursor()
	cur.execute("SELECT * FROM "+tname)
	rows = cur.fetchall()
	#print rows
	cur.execute("DELETE FROM "+tname)
	for row in rows:
		##"INSERT INTO Cars VALUES(1,'Audi',52642)"
		q = "INSERT INTO "+"rrobin_part"+str(lin)+" VALUES("+str(row[0])+", "+str(row[1])+", "+str(row[2])+")"
		#print q
		cur.execute(q)
		lin=(lin+1)%n
	
	con.commit()

def roundrobinpartition(tname, n, con):
	
	global part
	part = n
	cur = con.cursor()
	
	for i in range(0,n):
		cur.execute("DROP TABLE IF EXISTS "+"rrobin_part"+str(i))
	   	cur.execute("CREATE TABLE "+"rrobin_part"+str(i)+" () INHERITS ("+tname+");");
		
	part_create_rr(tname,n,con)
    
        con.commit()


def roundrobininsert(tname, userid, itemid, rating, con):
    global lin
    global part
    cur = con.cursor()
	
#"INSERT INTO Cars VALUES(1,'Audi',52642)"
    st = "INSERT INTO "+"rrobin_part"+str(lin)+" VALUES("+str(userid)+", "+str(itemid)+", "+str(float(rating))+")"
    
    cur.execute(st);
    lin=(lin+1)%part
    con.commit()

#table name hard-coded
def deletepartitionsandexit(con):
	cur = con.cursor()
	cur.execute("DROP TABLE IF EXISTS ratings CASCADE")
	#print "dropped"


def rangeinsert(tname, userid, itemid, rating, con):
    cur = con.cursor()
#"INSERT INTO Cars VALUES(1,'Audi',52642)"
    st = "INSERT INTO "+tname+" VALUES("+str(userid)+","+str(itemid)+","+str(float(rating))+")"
    
    #print st 
    cur.execute(st);
    
    cur.execute("SELECT * FROM range_part3");
    rows2 = cur.fetchall()
    for row2 in rows2:
	print row2
    
    #con.commit()
    con.commit()

# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings('ratings','test_data.dat', con)
	        rangepartition('ratings', 5, con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
			
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
