import sqlite3
import os
import collections
connection = None
cursor = None

def connect(path):
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
                k = k[1:-1]
                output.append([j,k])
        return output

def bcnf(inputRelationDict):
        print("\nSchemas:")
        for i,j in enumerate(inputRelationDict):
                print(str(i+1)+": "+j)
        option = int(input("Select schema: "))-1
        attributes = attributes_to_list(inputRelationDict[list(inputRelationDict.keys())[option]][0])
        fd = fd_to_list(inputRelationDict[list(inputRelationDict.keys())[option]][1])


        return

def union(schemaList):
        fdList = list()
        for sch in schemaList:
            cursor.execute("SELECT FDs FROM InputRelationSchemas WHERE Name = :sch", {"sch":sch})
            List = cursor.fetchall()[0][0].split(";")
            for i in List:
                j,k = i.split("=>")
                j = set(j.strip()[1:-1].split(','))
                k = set(k.strip()[1:-1].split(','))

                #if not empty
                if len(fdList) != 0:
                        # check if already in list
                        boolIn = False
                        for fdIndex in range(0,len(fdList)):
                                if j == fdList[fdIndex][0]:
                                        fdList[fdIndex][1].update(k)
                                        boolIn = True
                                        break
                        # if not in, add
                        if boolIn == False:
                                fdList.append([j,k])
                else:
                        fdList.append([j,k])
        return fdList


def closure(inputRelationDict):
        attributes = set(input("enter the attributes:\n (seperate with a comma between each attribute)\n").split(","))
        cursor.execute("SELECT Name FROM InputRelationSchemas")

        schemas = cursor.fetchall()
        for i in range(len(schemas)):
            print(str(i+1) + ". " + schemas[i][0])

        Choices = input("Select the schema(s) by enter the number\n").split(",")

        Schemas = []
        for choice in Choices:
            Schemas.append(schemas[int(choice)-1][0])

        att_check = []
        for att in attributes:
            Check = False
            for sch in Schemas:
                cursor.execute("SELECT Attributes FROM InputRelationSchemas WHERE Attributes like ? and Name = ?", ('%'+att+'%', sch))
                con = cursor.fetchone()
                if con != None:
                    Check = True
            att_check.append(Check)
        if False in att_check:
            print("certain attributes not inside selected schemas")
            return
        FDs = union(Schemas)
        print(FDs)
        closure = attributes
        closure = calculate_closure(FDs, attributes, closure)
        attributes = sorted(list(attributes))
        closure = sorted(list(closure))
        print("\nattributes: {" , end = "")
        for i in range(0, len(attributes)-1):
            print(attributes[i] + ',', end = "")
        print(attributes[-1] + "}")

        print("\nclosure: {", end = "")
        for i in range(0, len(closure)-1):
            print(closure[i] + ',', end = "")
        print(closure[-1] + "}")
        return

def calculate_closure(FDs, attributes, closure):
        change = False
        for j in FDs:
            if j[0].issubset(attributes) and j[1].issubset(attributes) == False:
                closure = closure | j[1]
                change = True
        if change == False:
            return closure
        attributes = attributes | closure
        closure = calculate_closure(FDs, attributes, closure)

        return closure



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
                                connect(input("Enter database name: "))
                        else:
                                connect(directory())
                        query = cursor.execute("Select Name,Attributes,FDs,hasInstance from InputRelationSchemas;").fetchall()
                        inputRelationDict = collections.OrderedDict()
                        for i in query:
                                inputRelationDict[i[0]] = [i[1],i[2],i[3]]
                        option = input("1: Perform BCNF on schema\n2: Compute attribute closure on schema\n3: Determine equivalency on two sets of functional dependencies\n0: Exit\nSelect option: ")
                        if option == '0':
                                exit()
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
