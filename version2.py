import sqlite3
import os
import collections
import sys
connection = None
cursor = None

def connect(path):
        if os.path.exists(path) == False:
                return False        
        global connection, cursor

        #Initialize the global variable 'connection' with a connection to the dadabase specified by 'path'
        print("Making connection to database ... ", end = '')
        
        connection = sqlite3.connect(path)
        print('Done')
        #Initialize the global variable 'cursor' with a cursor to the database you just connected
        print("Initializing cursor ... ", end = '')
        cursor = connection.cursor()
        print('Done')
        #Create and populate table is the database using 'init.sql' (from eclass)
        

        cursor.execute(' PRAGMA foreign_keys=ON; ')
        #print("Importing table ... ", end = '')
        #sqlcommand = open("table.sql").read()
        #cursor.executescript(sqlcommand)
        connection.commit()
        print("Done \n")

        return

def directory():
        arr = os.listdir()
        for i,j in enumerate(arr):
                if j[-3:] != ".db" and j[-9:] != ".sqliteDB":
                        arr.remove(j)
        for i,value in enumerate(arr,1):
                print(str(i)+": "+value)
        path = arr[int(input("Select valid database: "))-1]
        print("\n")
        return path

def attributes_to_list(attributes):
        return attributes.split(",")

def fd_to_list(fd):
        fd = list(fd.split("; "))
        output = []
        for i in fd:
                j,k = i.split("=>")
                j = j[1:-1]
                j = list(j.split(','))
                k = k[1:-1]
                k = list(k.split(','))
                output.append([j,k])
        return output

def bcnf(inputRelationDict):
        print("\nSchemas:")
        for i,j in enumerate(inputRelationDict):
                print(str(i+1)+": "+j)
        option = int(input("Select schema: "))-1
        attributes = attributes_to_list(inputRelationDict[list(inputRelationDict.keys())[option]][0])
        fd = fd_to_list(inputRelationDict[list(inputRelationDict.keys())[option]][1])
        i = 0
        while i < len(fd):
                if len(fd[i][1]) > 1:
                        temp = [fd[i][1].pop(0)]
                        fd.insert(i,[fd[i][0],temp])
                i+= 1
        duplicates = []
        for j0,j in enumerate(fd):
                k = 0
                while k < len(j[0]):
                        temp = j[0][k]
                        j[0][k] = None
                        closure = set(j[0])
                        closure.discard(None)
                        change = True
                        while change == True:
                                change = False
                                for m0,m in enumerate(fd):
                                        if m0 != j0:
                                                if set(m[0]).issubset(closure) and closure.intersection(set(m[1])) != set(m[1]):
                                                        change = True
                                                        closure.update(set(m[1]))
                        if temp in closure:
                                j[0].pop(k)
                        else:
                                j[0][k] = temp
                        k += 1
                if fd.count(j) > 1:
                        j = None
                        duplicates.append(j0)
        for n in reversed(duplicates):
                fd.pop(n)
        unnecessary = []
        for p0,p in enumerate(fd):
                closure = set(p[0])
                change = True
                while change == True:
                        change = False
                        for q0,q in enumerate(fd):
                                if p0 != q0:
                                        if set(q[0]).issubset(closure) and closure.intersection(set(q[1])) != set(q[1]):
                                                change = True
                                                closure.update(set(q[1]))
                if set(p[1]).issubset(closure):
                        unnecessary.append(p0)
        for r in reversed(unnecessary):
                fd.pop(r)
        s = len(fd) - 1
        while s >= 0:
                for t0,t in enumerate(fd):
                        if set(fd[s][0]) == set(t[0]) and s != t0:
                                t[1] += fd[s][1]
                                fd.pop(s)
                s -= 1
        print(fd)
        return

def closure(inputRelationDict):
        return

def equivalence(inputRelationDict):
        return 

def main_interface():
        while True:
                option = input("1: Enter database name\n2: Select database from current directory\n0: Exit\nSelect option: ")
                print("\n")
                if option == '0':
                        exit()
                elif option == '1' or option == '2':
                        if option == '1':
                                while True:
                                        name = input("Enter database name: ")
                                        if connect(name) != False:
                                                break
                                        print("That is an invalid database name.")
                                        

                        else:
                                connect(directory())

                        query = cursor.execute("Select Name,Attributes,FDs,hasInstance from InputRelationSchemas;").fetchall()
                        inputRelationDict = collections.OrderedDict()
                        for i in query:
                                inputRelationDict[i[0]] = [i[1],i[2],i[3]]
                        option = input("1: Perform BCNF on schema\n2: Compute attribute closure on schema\n3: Determine equivalency on two sets of functional dependencies\n0: Exit\nSelect option: ")
                        if option == '0':
                                sys.exit()
                        elif option == '1':
                                bcnf(inputRelationDict)
                        elif option == '2':
                                closure(inputRelationDict)
                        elif option == '3':
                                equivalence(inputRelationDict)
                print("Invalid key\n")
        return  


def main():
        global connection, cursor        

        main_interface()
        connection.close()
        return


if __name__ == "__main__":
        main()
