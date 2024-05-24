from pymongo import MongoClient,errors
from pymongo import UpdateMany,DeleteMany

class Abastecimentos():

    def __init__(self,host="localhost",port=27017): # mongo's default config
        '''

        :param host: O host onde o servidor de banco de dados está em execução. Se Nenhum, o host padrão será "localhost".
        :param port: A porta onde o host está escutando. Se Nenhum, a porta de escuta padrão será 27017.

        '''
        # cria o cliente mongo
        print("tentando se conectar ao MongoDB...")
        try:
            client = MongoClient(host=host,port=port)
            print("MongoDB conectado: " + str(client.address[0]) + ":" + str(client.address[1]))

            self.client = client
        except errors.ServerSelectionTimeoutError:

            print("Tempo limite do banco de dados... reinicie o módulo...")

        except:
            print("Erro desconhecido")

    def get_abastecimento(self, id):
        '''

        :param id: O id do abastecimento que estamos pesquisando.
        :return: Retorna uma lista de dados dos abastecimentos no formato: {"id":str, "ibm":str, "dthr":str, "val":str}

        '''
        abastecimentos = self.client.abastecimento_db.abastecimentos

        l = []

        for c in abastecimentos.find({'id':id}):
            l.append(c)


        self.client.close()
      
        return l

    def get_all_abastecimentos(self):
        '''

        :return: Retorna uma lista de dictos dos abastecimentos no formato: {"id":str, "ibm":str, "dthr":str, "val":str}

        '''
        abastecimentos = self.client.abastecimento_db.abastecimentos

        l = []


        for c in abastecimentos.find():
            l.append(c)

        self.client.close()
        
        return l

    def insert_abastecimento(self,obj):
        '''

        :param obj: O json do novo abastecimento.
        :return: Retorna um dict contendo todos os dados incluídos mais o ID gerado no MongoDB

        '''

        data = {'id':obj['id'],
                'ibm':obj['ibm'],
                'dthr':obj['dthr'],
                'val':obj['val']}

        abastecimentos = self.client.abastecimento_db.abastecimentos
        abastecimento_id = abastecimentos.insert_one(data).inserted_id
        self.client.close()

        data['_id'] = str(abastecimento_id)

        return data

    def delete_abastecimentos(self,id):
        '''

        :param id: O id do abastecimento que buscaremos excluir.
        :return: Retorna um dict com o status e o número de arquivos excluídos

        '''
        abastecimentos = self.client.abastecimento_db.abastecimentos

        # pode ser usado com mais de uma operação em uma lista de operações
        operations = [DeleteMany({'id': id})]

        results = abastecimentos.bulk_write(operations)

        print("{} object deleted.".format(results.deleted_count))

        return {"status":"sucess","deleted_count":results.deleted_count}

    def update_abastecimento(self,id_filter,new_obj):
        '''

        :param id_filter: O id do abastecimento que buscaremos para atualização.
        :param new_obj: O novo json para este abastecimento.
        :return: Retorna um dict com os novos dados mais o número de arquivos modificados.
        '''
        abastecimentos = self.client.abastecimento_db.abastecimentos

        new = {"id":new_obj['id'],
               "ibm":new_obj['ibm'],
               "dthr":new_obj['dthr'],
               "val":new_obj['val']}

        operations = [UpdateMany({'id': id_filter},{"$set":new})]

        results = abastecimentos.bulk_write(operations)
        print("{} objeto modificado.".format(results.modified_count))

        new["modified_count"] = results.modified_count

        return new

# teste de conexão
if __name__ == '__main__':

    #client = MongoClient(host="localhost",port=27017)
    #try:
    #    print("Connected - "+str(client.address[0])+":"+str(client.address[1]))
    #except errors.ServerSelectionTimeoutError:
    #    print("Database Timeout")

    db = Abastecimentos()
    print(db.get_all_abastecimentos())