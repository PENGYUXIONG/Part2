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
                j = set(j.split(','))
                k = k[1:-1]
                k = set(k.split(','))
                output.append([j,k])
        return output

#def bcnf(inputRelationDict):
        #print("\nSchemas:")
        #for i,j in enumerate(inputRelationDict):
                #print(str(i+1)+": "+j)
        #option = int(input("Select schema: "))-1
        #attributes = attributes_to_list(inputRelationDict[list(inputRelationDict.keys())[option]][0])
        #fd = fd_to_list(inputRelationDict[list(inputRelationDict.keys())[option]][1])
        #i = 0
        #while i < len(fd):
                #if len(fd[i][1]) > 1:
                        #temp = [fd[i][1].pop(0)]
                        #fd.insert(i,[fd[i][0],temp])
                #i+= 1
        #duplicates = []
        #for j0,j in enumerate(fd):
                #k = 0
                #while k < len(j[0]):
                        #temp = j[0][k]
                        #j[0][k] = None
                        #closure = set(j[0])
                        #closure.discard(None)
                        #change = True
                        #while change == True:
                                #change = False
                                #for m0,m in enumerate(fd):
                                        #if m0 != j0:
                                                #if set(m[0]).issubset(closure) and closure.intersection(set(m[1])) != set(m[1]):
                                                        #change = True
                                                        #closure.update(set(m[1]))
                        #if temp in closure:
                                #j[0].pop(k)
                        #else:
                                #j[0][k] = temp
                        #k += 1
                #if fd.count(j) > 1:
                        #j = None
                        #duplicates.append(j0)
        #for n in reversed(duplicates):
                #fd.pop(n)
        #unnecessary = []
        #for p0,p in enumerate(fd):
                #closure = set(p[0])
                #change = True
                #while change == True:
                        #change = False
                        #for q0,q in enumerate(fd):
                                #if p0 != q0:
                                        #if set(q[0]).issubset(closure) and closure.intersection(set(q[1])) != set(q[1]):
                                                #change = True
                                                #closure.update(set(q[1]))
                #if set(p[1]).issubset(closure):
                        #unnecessary.append(p0)
        #for r in reversed(unnecessary):
                #fd.pop(r)
        #print(fd)
        #return

def bcnf(inputRelationDict):
        print("\nSchemas:")
        for i,j in enumerate(inputRelationDict):
                print(str(i+1)+": "+j)
        option = int(input("Select schema: "))-1
        attributes = [set(attributes_to_list(inputRelationDict[list(inputRelationDict.keys())[option]][0]))]
        fd = [fd_to_list(inputRelationDict[list(inputRelationDict.keys())[option]][1])]
        print(list(inputRelationDict.keys())[option])
        i = 0
        d = 0
        while i < len(fd):
                j = 0
                while j < len(fd[i]):
                        s = set()
                        s.update(calculate_closure(fd[i],fd[i][j][0],fd[i][j][0]))
                        if set(attributes[i]).issubset(s) == False:
                                t = fd[i][j][1] - fd[i][j][0]
                                u = fd[i].pop(j)
                                k = 0
                                while k < len(fd[i]):
                                        fd[i][k][1] = fd[i][k][1] - t
                                        if len(fd[i][k][0].intersection(t)) != 0:
                                                d = 1
                                                fd[i][k][0] = fd[i][k][0] - t
                                                if len(fd[i][k][0]) == 0:
                                                        fd[i].pop(k)
                                                        k -= 1
                                        k += 1
                                attributes[i] = attributes[i] - t
                                fd.append([u])
                                attributes.append(u[0].union(u[1]))
                        else:
                                j += 1

                i += 1
        for j,k in enumerate(fd):
                output_attributes = sorted(list(attributes[j]))
                name = list(inputRelationDict.keys())[option]+"_"+"_".join(output_attributes)
                outputAttributes = ",".join(output_attributes)
                functionalDependencies = ""
                for l in k:
                        if len(l[1]) != 0:
                                functionalDependencies += "{"+",".join(sorted(list(l[0])))+"}=>{"+",".join(sorted(list(l[1])))+"}; "
                functionalDependencies = functionalDependencies[:-2]
                cursor.execute("Insert into OutputRelationSchemas values (?,?,?)",[name,outputAttributes,functionalDependencies])
        query = cursor.execute("Select * from OutputRelationSchemas").fetchall()

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
