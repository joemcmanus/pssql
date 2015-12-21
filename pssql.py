#!/usr/bin/env python
#pssql.py - Command Line MS SQL Server Client
#v0.03 2012/08/10 
#Copyright (C) 2012 Joe McManus josephmc at alumni.cmu.edu
#This program is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.
#
#This program is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License
#along with this program.  If not, see <http://www.gnu.org/licenses/>.


import pymssql
import ConfigParser
import sys
import readline
import atexit
import os 

version="version 0.2"
logging="no" #Default logging behaviour
paging="yes" #Default paging/more behaviour
histFile=os.path.join(os.path.expanduser("~"), ".pssqlhist") #command history

#Set up the command history
try:
	readline.read_history_file(histFile)
except IOError:
	pass
atexit.register(readline.write_history_file, histFile)

#Read the mssql.conf file
def readConf():
	config=ConfigParser.RawConfigParser()
	try:
		config.read('mssql.conf')
		dbUser=config.get("mssql", "user")
		dbPass=config.get("mssql", "pass")
		dbName=config.get("mssql", "dbName")
		dbHost=config.get("mssql", "host")
	except: 
		print("MS SQL Configurations options not set in mssql.conf")
		sys.exit()
	return dbUser, dbPass, dbName, dbHost

#Set up the db connection
def setDB(dbUser, dbPass, dbName, dbHost):
	try:
		db = pymssql.connect(host=dbHost, user=dbUser, password=dbPass, database=dbName)
		return db
	except Exception,e: 
		print("ERROR: Unable to connect to MsSQL Server")
		print(e)
		sys.exit()

#Display a help message
def displayHelp(): 
	print("pssql: Command Line MS SQL Server Client \n" \
	+ version + " Joe McManus josephmc@alumni.cmu.edu \n" \
	+ "Usage: ./pssql.py (optional: dbUser dbPass dbName dbHost !overrides setting in mssql.conf) \n" \
	+ " -- commands: \n" \
	+ "	paging	Sets page break on or off, similar to more \n" 
	+ "	log	turns logging on or off\n" 
	+ "	quit	Exits prompt \n"
	+ " 	help	displays this message \n")

#Handle results
def displayResults(sqlResult, logging): 
	#determine size of screen, set column width accourdingly
	rows, columns = os.popen('stty size', 'r').read().split()
	colWidth=round(int(columns)/len(sqlResult),0)-3
	colWidth=int(colWidth)
	i=0
	while( i < len(sqlResult)): 
		prResult=str(sqlResult[i])
		prResult=prResult[:colWidth]
		print(prResult.rjust(colWidth)) + "|", 		
		i+=1
	print (' ')
	if logging == "yes": 
		i=0
		while( i < len(sqlResult)):
			fh.write(str(sqlResult[i]) + ",")
			i+=1
		fh.write("\n")
			
#Handle command line options
def commandLineOptions():
	dbUser=sys.argv[1]
	dbPass=sys.argv[2]
	dbName=sys.argv[3]
	dbHost=sys.argv[4]
	print("Attempting to use database " + dbName + " on host " + dbHost)
	return dbUser, dbPass, dbName, dbHost

#Turn logging on and off
def setLogging(outFile, action, fh):
	if action == "start": 
		print("Copying All Query Results to CSV file:" + outFile) 
		try:
			fh=open(outFile, 'w')
		except:
			print("ERROR: Unable to write to" + outFile)
			sys.exit()
		return fh
	else: 
		fh.close()
		print("Stopped logging to file:" + outFile) 

#If db connections details are passed in on command line override mssql.conf
if len(sys.argv) == 5:
	dbUser, dbPass, dbName, dbHost = commandLineOptions()
else: 
	dbUser, dbPass, dbName, dbHost = readConf()

db=setDB(dbUser, dbPass, dbName, dbHost)

cur = db.cursor()
print "Python MS SQL Client"
print "--------------------"

while True: 
	try:
		pyQuery = raw_input("Query : ")
		if pyQuery == "quit": 
			sys.exit()
		elif pyQuery == "paging":
			if paging == "yes": 
				paging = "no"
				print("Paging set to OFF")
			else:
				paging = "yes"
				print("Paging set to ON")
		elif pyQuery == "log":
			if logging == "yes":
				setLogging(outFile, 'stop', fh)
				logging="no"
			else: 
				outFile=raw_input("Log File Name: ")
				fh=setLogging(outFile, 'start', '')
				logging="yes"

		elif pyQuery == "help": 
			displayHelp()
		else:
			rows, columns = os.popen('stty size', 'r').read().split()
			j=0
			try:
				cur.execute(pyQuery)
				row = cur.fetchone()
				while row:
    					displayResults(row, logging)
    					row = cur.fetchone()
					if paging == "yes":
						if str(j) == str(rows) : 
							foo=raw_input("--More--")
							j=0
						else : 
							j+=1
					
			except Exception,e:
				print(e)
	
	except KeyboardInterrupt: #Handle Ctrl-C
		print("Exiting pssql");
		sys.exit()
db.close()

