import pymongo

from caja import Password
from crudmysql import MySQL
from conf import variables
from Variables import variablesSQL as varsmysql
from mongodb import MongoDB


def cargar_estudiantes():
    obj_MySQL = MySQL(varsmysql)
    obj_Mongo = MongoDB(variables)
    sql_estudiante = "SELECT * FROM estudiantes;"
    sql_kardex = "SELECT * FROM kardex;"
    sql_usuarios = "SELECT * FROM usuarios"
    obj_MySQL.conectar_mysql()
    lista_estudiantes = obj_MySQL.consulta_sql(sql_estudiante)
    lista_kardex = obj_MySQL.consulta_sql(sql_kardex)
    lista_usuarios = obj_MySQL.consulta_sql(sql_usuarios)
    obj_MySQL.desconectar_mysql()
    obj_Mongo.conectar_mongodb()
    for est in lista_estudiantes:
        e = {
            "control": est[0],
            "nombre": est[1]
        }
        print(e)
        obj_Mongo.insertar('estudiantes', e)
    obj_Mongo.desconectar_mongodb()
    obj_Mongo.conectar_mongodb()
    for mat in lista_kardex:
        m = {
            "idKardex": mat[0],
            "control": mat[1],
            "materia": mat[2],
            "calificacion": float(mat[3])
        }
        print(m)
        obj_Mongo.insertar('kardex', m)
    obj_Mongo.desconectar_mongodb()
    obj_Mongo.conectar_mongodb()
    for usr in lista_usuarios:
        u = {
            "idUsuario": usr[0],
            "nombre": usr[1],
            "clave": usr[2],
            "clave_cifrada": usr[3]
        }
        print(u)
        obj_Mongo.insertar('usuarios', u)
    obj_Mongo.desconectar_mongodb()

def insertar_estudiante():
    obj_Mongo = MongoDB(variables)
    print("########### INSERTAR ESTUDIANTE ##############")
    ctrl = input("Dame el numero de control")
    nombre = input("Dame el nombre del estudiante")
    clave = input("Dame la clave de acceso")
    obj_usuario = Password(longitud=len(clave), contrasena=clave)
    json_estudiante = {'control': ctrl,'nombre':nombre}
    json_usuario = { 'idUsuario':100,'control':ctrl,'clave':clave,'clave_cifrada':obj_usuario.contrasena_cifrada.decode()}
    obj_Mongo.conectar_mongodb()
    obj_Mongo.insertar('estudiantes',json_estudiante)
    obj_Mongo.insertar('usuarios', json_usuario)
    obj_Mongo.desconectar_mongodb()
def consultar_materias():
    obj_PyMongo = MongoDB(variables) # *********************************
    print(" == CONSULTAR MATERIAS POR ESTUDIANTE ==")
    ctrl = input("Dame el numero de control: ")
    filtro={ 'control':ctrl}
    atributos_estudiente={"_id":0,"nombre":1}
    atributos_kardex={"_id":0,"materia":1,"calificacion":1}
    #sql_materias = "SELECT E.nombre, K.materia, K.calificacion " \
                   #"FROM estudiantes E, kardex K " \
                   #f"WHERE E.control = K.control and E.control='{ctrl}';"
    obj_PyMongo.conectar_mongodb()
    respuesta1 = obj_PyMongo.consulta_mongodb('estudiantes',filtro,atributos_estudiente)
    respuesta2=obj_PyMongo.consulta_mongodb('kardex',filtro,atributos_kardex)
    obj_PyMongo.desconectar_mongodb()
    #print(f"respuesta1:",respuesta1)
    #print(f"respuesta2:", respuesta2)
    if respuesta1["status"] and respuesta2["status"]:
        print("Estudiantes: ",respuesta1["resultado"][0]["nombre"])
        for mat in respuesta2["resultado"]:
            print(mat["materia"],mat["calificacion"])
def actualizar_calificacion():
    obj_Mongo = MongoDB(variables)
    print("########### ACTUALIZAR CALIFICACION ESTUDIANTE ##############")
    ctrl = input("Dame el numero de control")
    materia = input("Dame el nombre de la materia")
    filtro_materia={'control': ctrl,'materia': materia}
    obj_Mongo.conectar_mongodb()
    mongo_buscar_materia = obj_Mongo.consulta_mongodb('kardex',filtro_materia)
    if mongo_buscar_materia:
        print("Encontrado")
        print(mongo_buscar_materia)

        #promedio = float(input("Dame el nuevo promedio: "))
        #     sql_actualiza_prom = f"UPDATE kardex set calificacion={promedio} " \
        #                          f"WHERE control='{ctrl}' and materia='{materia.strip()}';"
        #     obj_MySQL.conectar_mysql()
        #     obj_MySQL.consulta_sql(sql_actualiza_prom)
        #     obj_MySQL.desconectar_mysql()
        #     print("Promedia ha sido actualizado")
    else:
        print('No encontrado')
        print(f"El estudiante con numero de control {ctrl} o la materia: {materia} NO EXISTE")
    obj_Mongo.desconectar_mongodb()
def Consulta_generalmongo():
    obj_Pymongo = MongoDB(variables)
    obj_Pymongo.conectar_mongodb()
    print("CONSULTA GENERAL")
    respuesta = obj_Pymongo.consultageneral_mongodb("estudiantes")
    respuesta2 = obj_Pymongo.consultageneral_mongodb("kardex")
    obj_Pymongo.desconectar_mongodb()
    i = 0;
    if respuesta["status"] and respuesta2["status"]:
        for res1 in respuesta["resultado"]:
            j=0
            prom = 0
            cont = 0
            for res2 in respuesta2["resultado"]:
                if respuesta["resultado"][i]["control"] == respuesta2["resultado"][j]["control"]:
                    prom += respuesta2["resultado"][j]["calificacion"]
                    cont += 1
                j+=1
            if(cont>0):
                prom = prom/cont
            print("CONTROL: ", respuesta["resultado"][i]["control"], " NOMBRE: ", respuesta["resultado"][i]["nombre"], " PROMEDIO: ", prom)
            i +=1
   # mongo_checar = obj_Mongo.consulta_mongodb(mongo_buscar_materia)
    #print(mongo_checar)
    #checar = (mongo_checar[0])[0]
    #print(checar)
    #if checar == 1:
    #    calificacion = int(input("Inserte nueva calificacion"))
    #    sql_cal = f"UPDATE kardex SET calificacion = {calificacion} WHERE control = '{ctrl}' AND materia='{materia}' "
    #    resultado = obj_MySQL.consulta_sql(sql_cal)
    #    print("Actualizacion exitosa \n")
    #else:
    #    print("Error en los datos del Alumno o Materia")

def eliminar_estudiante():
    obj_Mongo = MongoDB(variables)
    print("########### ELIMINAR ESTUDIANTE ##############")
    ctrl = input("Dame el numero de control")
    json_estudiante = {'control': ctrl}
    obj_Mongo.conectar_mongodb()
    var=obj_Mongo.consulta_mongodb('estudiante',json_estudiante)
    if var["status"]:
        obj_Mongo.eliminar('estudiantes',json_estudiante)
        obj_Mongo.eliminar('kardex', json_estudiante)
        obj_Mongo.eliminar('usuarios', json_estudiante)
        print("Alumno eliminado exitosamente")
    else:
        print("Alumno no ha sido encontrado")
    obj_Mongo.desconectar_mongodb()


def menu():
    print("############# Menu Principal ################")
    print("1.Insertar estudiante \n2.Actualizar Calificacion \n3.Consultar materias por estudiante \n"
          "4.Consulta general de estudiantes\n5.Eliminar a un estudiante\n6.Salir")
    ban = True
    while ban:
        opcion = int(input("Escoge una opcion: "))
        match (opcion):
            case 1:
                insertar_estudiante()
            case 2:
                actualizar_calificacion()
            case 3:
                consultar_materias()
            case 4:
                Consulta_generalmongo()
            case 5:
                eliminar_estudiante()
            case 6:
                ban = False


#cargar_estudiantes()
menu()