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

#simply removes , then returns attributes 
def attributes_to_list(attributes):
        return attributes.split(",")

#turns string into useful format
def fd_to_list(fd):
        #separates aech fd by ;
        fd = list(fd.split("; "))
        #to be returned
        output = []
        #iterates through fds
        for i in fd:
                #LHS is left of => RHS is right of
                j,k = i.split("=>")
                #removes {}
                j = j[1:-1]
                #removes ,
                j = set(j.split(','))
                k = k[1:-1]
                k = set(k.split(','))
                #adds to output as sets
                output.append([j,k])
        return output

#finds bcnf decomposition
def bcnf(inputRelationDict):
        #prints out each schema from the inputrelationschemas table for the user to choose
        print("\nSchemas:")
        for i,j in enumerate(inputRelationDict):
                print(str(i+1)+": "+j)
        #gets user input
        option = int(input("Select schema: "))-1
        #converts the attributes stored in the table into a useful format
        attributes = [set(attributes_to_list(inputRelationDict[list(inputRelationDict.keys())[option]][0]))]
        #as above but with functional dependencies
        fd = [fd_to_list(inputRelationDict[list(inputRelationDict.keys())[option]][1])]
        #stores fd to check if preserved
        fdCopy = list(fd[0])
        #i is the index of table that is under examination
        i = 0
        #will turn to true if there is a change in the list of tables
        change = False
        #goes through every table stored
        while i < len(fd):
                #j is index of functional dependency within table
                j = 0
                #goes through ever functional dependency in table
                while j < len(fd[i]):
                        #s stores closure given the set of attributes on the LHS of fd[i][j]
                        s = set()
                        s.update(calculate_closure(fd[i],fd[i][j][0],fd[i][j][0]))
                        #if s is not a superset of attributes in the table the LHS is not a superkey and the table is not in bcnf
                        if set(attributes[i]).issubset(s) == False:
                                #t is attributes on RHS but not LHS of fd at fd[i][j]
                                t = fd[i][j][1] - fd[i][j][0]
                                #removes fd at fd[i][j]
                                u = fd[i].pop(j)
                                #removes attributes in table that were part of RHS-LHS of the fd
                                attributes[i] = attributes[i] - t
                                #creates new table with the examined fd in it
                                fd.append([u])
                                #acknowledges that a change in stored tables was made
                                change = True
                                #stores attruibutes for new table
                                attributes.append(u[0].union(u[1]))                                
                                #k is index of fd in table
                                k = 0
                                #goes through every fd in table
                                while k < len(fd[i]):
                                        #removes set t from every RHS
                                        fd[i][k][1] = fd[i][k][1] - t
                                        fd[i][k][0] = fd[i][k][0] - t
                                        #checks if LHS is empty and is so removes it
                                        if len(fd[i][k][0]) == 0:
                                                fd[i].pop(k)
                                        #increments k
                                        else:
                                                k += 1
                        #increments index if s is a superset (which would not otherwise ahppen as an element is removed instead)
                        else:        
                                j += 1
                #goes to next table
                i += 1
                #if there was a change retries decomposing the fds in each table and resets i & change
                if i == len(fd) and change == True:
                        change = False
                        i = 0
        #checks if dependency preserving
        d = False
        #goes through every original fd
        for j in fdCopy:
                #s is the closure of the fd in question
                s = set()
                s.update(calculate_closure(fdCopy,j[0],j[0]))
                #t is the closure given the LHS in question if we use the fds from the new tables
                t = set()
                change = True
                #updates t until no change
                while change == True:
                        change = False
                        #iterates through every table in fd
                        for k in fd:
                                #if u is smalle rthan t afterwards there was a change
                                u = len(t)
                                #gets the closure of a table given the original LHS
                                t.update(calculate_closure(k,j[0],j[0]))
                                if u < len(t):
                                        change = True
                #if the closure if different the decomposition must not be dependency preserving
                if s != t:
                        print(fdCopy)
                        print(j)
                        print(s)
                        d = True
                        break
        #prints whether dependency preserving
        if d == False:
                print("The BCNF decomposition was dependency preserving.")
        else:
                print("The BCNF decomposition was not dependency preserving.")
        
        fkDict = {}
        #iterates through every fd
        for j,k in enumerate(fd):
                #sorts attributes and converts to list
                outputAttributes = sorted(list(attributes[j]))
                #formats name to rel_... 
                name = list(inputRelationDict.keys())[option]+"_"+"_".join(outputAttributes)
                #joins output attributes into string
                outputAttributes = ",".join(outputAttributes)
                #makes string of fds in correct format
                functionalDependencies = ""
                for l in k:
                        if len(l[1]) != 0:
                                functionalDependencies += "{"+",".join(sorted(list(l[0])))+"}=>{"+",".join(sorted(list(l[1])))+"}; "
                functionalDependencies = functionalDependencies[:-2]
                #inserts entry
                cursor.execute("Insert into OutputRelationSchemas values (?,?,?)",[name,outputAttributes,functionalDependencies])
                #pk = primary key
                pk = set()
                #goes through atribute in LHS of fd which is the primary key
                for l in k:
                        pk.update(l[0])
                pk = sorted(list(pk))
                #fk = foreign key
                fk = []
                for l in pk:
                        if l in fkDict:
                                fk.append(", Foreign key ("+l+") References "+fkDict[l])
                        else:
                                fkDict[l] = name
                if len(fk) == 0:
                        fk = ""
                else:
                        fk = "".join(fk)
                #joins primary key 
                a = ",".join(pk)
                #gets name of original table
                b = list(inputRelationDict.keys())[option]
                #creates table
                cursor.execute("Create table "+name+" ("+outputAttributes+", Primary key ("+a+")"+fk+")")
                #insert old entries into new table replace if duplicate
                cursor.execute("Insert  into "+name+" select distinct "+outputAttributes+" from "+b)

        #commits to database
        connection.commit()
        return

def closure(inputRelationDict):
        # get input attributes
        attributes = set(input("enter the attributes:\n (seperate with a comma between each attribute)\n").split(","))
        cursor.execute("SELECT Name FROM InputRelationSchemas")

        schemas = cursor.fetchall()
        for i in range(len(schemas)):
            print(str(i+1) + ". " + schemas[i][0])

        # get input schemas
        Choices = input("Select the schema(s) by enter the number\n").split(",")

        Schemas = []
        for choice in Choices:
            Schemas.append(schemas[int(choice)-1][0])

        # check if inputs are valid
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
        # get union for the FDs
        FDs = union(Schemas)
        closure = attributes
        # take closure calculation
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

def equivalence(inputRelationDict):
        #User selecting schema
        print("\nSchemas' name in InputRelationSchemas:")

        # list of schemas for selecting
        schemaNameList = list()
        index = int(1)
        for schema in inputRelationDict:
                schemaNameList.append(schema)
                print(index,": ",schema)
                index += 1

        schemaNameList.append("Exit")
        print(index,": ","Exit")

        # collect user input
        slctNameListF1 = equivalence_user_input_collection(schemaNameList,index,"F1")
        slctNameListF2 = equivalence_user_input_collection(schemaNameList,index,"F2")

        # show user what they select
        print("Schema in F1:")
        for name in slctNameListF1:
                print(name)
        print("Schema in F2:")
        for name in slctNameListF2:
                print(name)


        # union their fd
        fdListF1 = union(slctNameListF1)
        fdListF2 = union(slctNameListF2)
        # print(fdListF1)
        # print(fdListF2)

        # print(calculate_closure(fdListF1, {'A','B'}, {'A','B'}))
        # print(calculate_closure(fdListF2, {'A','B'}, {'A','B'}))

        # compare F1 to F2
        boolAllInF1 = True
        for fd in fdListF1:
                # if not directly match
                if fd not in fdListF2:
                        # check closure of F2
                        closure = calculate_closure(fdListF2,fd[0],fd[0])
                        # if not in closure
                        if not fd[1].issubset(closure):
                                boolAllInF1 = False
                                print("F1",fd)
                                break

        # compare F2 to F1
        boolAllInF2 = True
        for fd in fdListF2:
                # if not directly match
                if fd not in fdListF1:
                        # check closure of F1
                        closure = calculate_closure(fdListF1,fd[0],fd[0])
                        # if not in closure
                        if not fd[1].issubset(closure):
                                boolAllInF2 = False
                                print("F2",fd)
                                break

        # print(boolAllInF1,boolAllInF2)
        if boolAllInF1 == True and boolAllInF2 == True:
                result = "ARE"
        else:
                result = "ARE NOT"

        print("Two sets of functional dependencies F1 and F2 " + result + " equivalent. ")

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

def calculate_closure(FDs, attributes, closure):
        # boolean to see if there is more attribute to add
        change = False
        # check each layer
        for j in FDs:
            if set(j[0]).issubset(attributes) and set(j[1]).issubset(attributes) == False:
                closure = closure | j[1]
                change = True
        # not change means the search is over, return result
        if change == False:
            return closure
        # add attributes in 
        attributes = attributes | closure
        closure = calculate_closure(FDs, attributes, closure)

        return closure


def TEST():
        # testSet = {'a','b','c'}
        # testSet.update({'b','c','d'})
        # print(testSet)
        testList = []
        print(testList[0])

def equivalence_user_input_collection(schemaNameList,index,F):
        slctNameList = list()

        while(True):
                ## print("TESTING--slctNameList:",slctNameList)
                print("Enter index to select schema for",F,"(or exit):")
                slctIndex = int(input())
                # check if input out of index
                if (slctIndex <= 0 or slctIndex > index):
                        print("Invalid index, choose again")
                else:
                        ## print("TESTING--slctName:",schemaNameList[slctIndex - 1])
                        # check if input is chose before
                        if (schemaNameList[slctIndex - 1] in slctNameList):
                                print("The schema already selected, choose another one.")
                        else:
                                # check if input is Exit
                                if (schemaNameList[slctIndex - 1] == "Exit"):
                                        # if Exit, have user choose any schema
                                        if (len(slctNameList) == 0):
                                                print("You haven't select any schema yet.")
                                        else:
                                                break
                                # a valid schema name
                                else:
                                        slctNameList.append(schemaNameList[slctIndex - 1])
        return slctNameList


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
                        else:
                                print("Invalid key\n")
                else:
                        print("Invalid key\n")
        return


def main():
        global connection, cursor

        # TEST()

        main_interface()
        connection.close()
        return


if __name__ == "__main__":
        main()
