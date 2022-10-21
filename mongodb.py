#CLASE PARA CONECTARSE A MONGO

import pymongo
from crudmysql import MySQL
from conf import variables
from Variables import variablesSQL as varsmysql



from conf import variables
#from env import  variables as varmysql
class MongoDB():
    def __init__(self, variables):
        #(host='localhost', db='opensource', port=27017, timeout=1000,user='', pwd=''):
        #Crear cadena de conexión
        self.MONGO_DATABASE = variables['db']
        self.MONGO_URI = 'mongodb://' + variables["host"] + ':' + str(variables['port'])
        self.MONGO_CLIENT = None
        self.MONGO_RESPUESTA = None
        self.MONGO_TIMEOUT = variables['timeout']

    def conectar_mongodb(self):
        try:
            self.MONGO_CLIENT = pymongo.MongoClient(self.MONGO_URI, serverSelectionTimeoutMS=self.MONGO_TIMEOUT)
        except Exception as error:
            print("ERROR: ", error)
        else:
            print("Conexión al Servidor MONGO DB realizada")
        #finally:


    def desconectar_mongodb(self):
        if self.MONGO_CLIENT:
            self.MONGO_CLIENT.close()

    def consulta_mongodb(self, tabla,filtro):
        self.MONGO_RESPUESTA = self.MONGO_CLIENT[self.MONGO_DATABASE][tabla].find({filtro})
        if self.MONGO_RESPUESTA:
            return self.MONGO_RESPUESTA
            #for reg in self.MONGO_RESPUESTA:
            #    print(reg)

    def insertar_estudiante(self,est):
        self.MONGO_RESPUESTA = self.MONGO_CLIENT[self.MONGO_DATABASE]['estudiantes'].insert_one(est)
        if self.MONGO_RESPUESTA:
            return True
        else:
            return False
    def eliminar(self, tabla,filtro):
        response = {'status': False, "resultado": []}
        self.MONGO_RESPUESTA = self.MONGO_CLIENT[self.MONGO_DATABASE][tabla].delete_many(filtro)
        if self.MONGO_RESPUESTA:
            response['status'] = True
        return response

    def insertar(self,collection, reg):
        self.MONGO_RESPUESTA = self.MONGO_CLIENT[self.MONGO_DATABASE][collection].insert_one(reg)
        if self.MONGO_RESPUESTA:
            return self.MONGO_RESPUESTA
        else:
            return False


    # def cargar_estudiantes(self):
    #   obj_MySQL = MySQL(varmysql)
    #    obj_MySQL
    def cargar_Alumnos(self):
        archivo = open("Estudiantes.prn", "r")
        alumno = {}
        for x in archivo:
            ctrl = (x[0:8])
            nom = (x[8:].replace("\n", ""))
            alumno["control"] = ctrl
            alumno["nombre"] = nom
            self.insertar_estudiante(alumno)
            alumno.clear()
        print("Insercion Terminada")

    def cargar_estudiantes(self):
        obj_MySQL = MySQL(varsmysql)
        obj_Mongo = MongoDB(variables)
        sql_estudiante= "SELECT * FROM estudiantes;"
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
                "idKardex":mat[0],
                "control": mat[1],
                "materia": mat[2],
                "calificacion":float(mat[3])
            }
            print(e)
            obj_Mongo.insertar('kardex', m)
        obj_Mongo.desconectar_mongodb()
        obj_Mongo.conectar_mongodb()
        for usr in lista_kardex:
            u = {
                "idUsuario":usr[0],
                "nombre": usr[1],
                "clave": usr[2],
                "clave_cifrada": mat[3]
            }
            print(e)
            obj_Mongo.insertar('usuarios', u)
        obj_Mongo.desconectar_mongodb()

    def consulta_mongodb(self, tabla, filtro, atributos={"_id": 0}):
        response = {"status": False, "resultado": []}
        self.MONGO_RESPUESTA = self.MONGO_CLIENT[self.MONGO_DATABASE][tabla].find(filtro, atributos)
        if self.MONGO_RESPUESTA:
            response["status"] = True
            for reg in self.MONGO_RESPUESTA:
                # print(reg)
                response["resultado"].append(reg)
            # return self.MONGO_RESPUESTA
            # for reg in self.MONGO_RESPUESTA:
            #    print(reg)                                     #ARMAR JSON
        return response

    def consultageneral_mongodb(self, tabla):
        response = {"status": False, "resultado": []}
        self.MONGO_RESPUESTA = self.MONGO_CLIENT[self.MONGO_DATABASE][tabla].find({})
        if self.MONGO_RESPUESTA:
            response["status"] = True

            for reg in self.MONGO_RESPUESTA:
                response["resultado"].append(reg)

        return response
alumno ={
    "control":300,
    "nombre":""
}

obj_Mongo = MongoDB(variables)
obj_Mongo.conectar_mongodb()
#obj_Mongo.insertar_estudiante(alumno)
#obj_Mongo.consulta_mongodb()
obj_Mongo.cargar_Alumnos()
obj_Mongo.desconectar_mongodb()