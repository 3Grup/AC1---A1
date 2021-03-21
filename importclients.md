*PART A OPCIO 2

import mysql.connector
from mysql.connector import errorcode
import csv
import random
random.seed()

try:
    #Conectamos a la base de datos.
    mydb = mysql.connector.connect(
        host="192.168.56.102", user="perepi", password="pastanaga", db="db_hotels"
        )
    #Activamos el cursor encargado de subir los datos.
    cursor = mydb.cursor()
    #Creamos una variable primera linea para saltarnos en client_id etc.
    firstline=True
    
    #Si queremos eliminar y crear de nuevo la tabla clients descomentaremos estos campos.

    #command = "ALTER TABLE reserves DROP COLUMN client_id, DROP FOREIGN KEY FK_reserves_clients"
    #cursor.execute(command)
    #mydb.commit()
    #command = "DROP TABLE IF EXISTS clients"
    #cursor.execute(command)
    #mydb.commit()
    
    #Creamos la tabla correspondiente a cleintes, con sus correspondietes PK y FK.
    #command="CREATE TABLE clients (client_id INT UNSIGNED NOT NULL AUTO_INCREMENT, nom VARCHAR(20), cognom1 VARCHAR(30), sexe ENUM('M','F'), data_naixement DATE, pais_origen_id TINYINT(3) UNSIGNED NOT NULL, CONSTRAINT PK_client PRIMARY KEY (client_id), CONSTRAINT FK_clients_paisos FOREIGN KEY (pais_origen_id) REFERENCES paisos (pais_id) ON UPDATE CASCADE ON DELETE CASCADE)"
    #cursor.execute(command)
    #mydb.commit()
    #Creamos el vinculo de la tabla reservas con la tabla clientes a traves de una nueva FK y su nueva columna client_id.
    #command="ALTER TABLE reserves ADD COLUMN client_id INT UNSIGNED, ADD CONSTRAINT FK_reserves_clients FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE ON UPDATE CASCADE" 
    #cursor.execute(command)
    #client_id = cursor.lastrowid
    #mydb.commit()

    #Asignamos un nombre a nuestro archivo situado en la misma posicion que se encuentra nuestro python.
    with open ("dades_clients-tab.csv") as csvfile:
        #Leemos el documento y se lo asignamos a una variable.
        reader = csv.reader(csvfile, delimiter='\t')
        #Iniciamos el bucle.               
        for row in reader:
            #Para eliminar la primera linea de nuestro archivo asignamos una variable false.
            if firstline:
                firstline=False
            #Comenzamos a escribir linea a linea nuestro codigo.
            #Realizamos dos busquedas para limpiar de ' y " en las row equivalentes a los nombres.
            elif "'" in row[1]:
                row[1]=row[1].replace("\'","")
                command = "INSERT INTO clients (client_id, nom, cognom1, sexe, data_naixement, pais_origen_id) "      
                command += f"VALUES ('{row[0]}','{row[1]}','{row[2]}','{row[3]}',STR_TO_DATE('{row[4]}','%d/%m/%Y'),'{row[5]}')"
                #Imprimiremos cada una de las lineas enviadas para saber cuando parara el programa.
                print(command)
                #Enviamos los datos a la base de datos.       
                cursor.execute(command)
                #Hacemos un commit por cada linea subida para asegurarnos de que cada una sube a la base de datos.
                mydb.commit()  
            elif '"' in row[1]:
                row[1]=row[1].replace("\"","")
                command = "INSERT INTO clients (client_id, nom, cognom1, sexe, data_naixement, pais_origen_id) "      
                command += f"VALUES ('{row[0]}','{row[1]}','{row[2]}','{row[3]}',STR_TO_DATE('{row[4]}','%d/%m/%Y'),'{row[5]}')"
                #Imprimiremos cada una de las lineas enviadas para saber cuando parara el programa.
                print(command)
                #Enviamos los datos a la base de datos.       
                cursor.execute(command)
                #Hacemos un commit por cada linea subida para asegurarnos de que cada una sube a la base de datos.
                mydb.commit()  
            #Escribimos un insert por cada una de las lineas de valores.
            #Con este apartado STR_TO_DATE('{row[4]}','%d/%m/%Y'), coseguimos subir los datos con la alteracion de año/mes/dia,
            #que requiere Mysql.
            else:
                command = "INSERT INTO clients (client_id, nom, cognom1, sexe, data_naixement, pais_origen_id) "      
                command += f"VALUES ('{row[0]}','{row[1]}','{row[2]}','{row[3]}',STR_TO_DATE('{row[4]}','%d/%m/%Y'),'{row[5]}')"
                #Imprimiremos cada una de las lineas enviadas para saber cuando parara el programa.
                print(command)
                #Enviamos los datos a la base de datos.       
                cursor.execute(command)
                #Hacemos un commit por cada linea subida para asegurarnos de que cada una sube a la base de datos.
                mydb.commit()
    #Crearemos otra variable para controlar que los numero no ser repitan a la hora de crear nuevas sentencias.
    llistaID=[]
    #Crearemos un centinela para parar el bucle cuando el largo de la lista sea el numero maximo de reservas id.
    parar=True
    #Ara crearem un bucle en el que client_id de la taula reserves tingui una reserva feta aleatoriament.    
    while parar:
        #Creamos dos variables random para que aleatoriamente posicione un la client_id en la posicion que corresponda,
        # de reserva_id 
        clientID = random.randint(10001,27944)    
        reservaID = random.randint(1,26990)
        #Si el numero de reservaID no esta dentro de llistaID, generame la sentencia sql.    
        if reservaID not in llistaID:
            command=f"UPDATE reserves SET client_id = '{clientID}' WHERE reserva_id = '{reservaID}'"
            #Agregame a la lista el numero que se a creado anteriormente en la variable reservaID.
            llistaID.append(reservaID)
            cursor.execute(command)
            mydb.commit()
            print(command)
        #Si no, mira el largo de lista, y si es igual a 26990(que es el numero maximo de reserves_id), 
        # para el bucle con el centinela.
        elif len(llistaID) == 26990:
            parar=False

#Aqui mostraremos los posibles errores dados por un fallo en la conexion.

except mysql.connector.Error as err:
    
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("El usuario o contraseña no es el correcto")
    
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("La base de datos no existe")
    
    else:
        print(err)
                
#Realizaremos de nuevo un commit por si aun quedase alguna linea por aceptar en la base de datos.
mydb.commit()
#Terminaremos el cursor.  
cursor.close()
#Terminaremos la conexion con la base de datos.
mydb.close()
print("Trabajo terminado, se ha terminado la conexion con la base de datos")
