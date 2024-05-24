from flask import Flask, request
from flask_restful import Resource, Api
from flask_httpauth import HTTPBasicAuth
from utils import Abastecimentos

app = Flask(__name__)
api = Api(app)

class Abastecimento(Resource):

    def get(self,ibm):

        # abre conexão no banco de dados e filtra os abastecimentos por ibm
        abastecimento = Abastecimentos().get_abastecimento(ibm)
        try:

            # prepara a lista de dictos para retornar na api
            response = [{"id": c['id'],"ibm":c['ibm'],"dthr":c['dthr'],"val":c['val']} for c in abastecimento]

        except AttributeError:
            response = {"status":"error","msg":"Nenhum abastecimento encontrado"}
        except:
            response = {"status":"error","msg":"Algo deu errado"}

        return response

    def put(self,ibm):

        # abre conexão no banco de dados
        abastecimento = Abastecimentos()

        # processar o corpo recebido na solicitação
        data = request.json

        try:
            # atualize os abastecimentos filtrando por nome com os dados recebidos
            response = abastecimento.update_abastecimento(name_filter=ibm,new_obj=data)
        except:
            response = {"status":"error","msg":"Algo deu errado"}

        return response

    def delete(self,id):

        # abre conexão no banco de dados
        abastecimento = Abastecimentos()

        try:
            # exclui todos os abastecimentos com o id fornecido
            response = abastecimento.delete_abastecimentos(id=id)
        except:
            response = {"status":"error","msg":"Algo deu errado"}

        return response


class ListAbastecimentos(Resource):

    def get(self):
        
        # abre conexão com o banco de dados e obtém todos os abasteciments existentes
        abastecimentos = Abastecimentos().get_all_abastecimentos()

        try:
            # prepara a lista de dictos para retornar na api
            response = [{"id": c['id'],"ibm": c['ibm'],"dthr": c['dthr'],"val": c['val']} for c in abastecimentos]

        except AttributeError:
            response = {"status": "error", "msg": "Nenhum abastecimento encontrado"}
        except:
            response = {"status": "error", "msg": "Algo deu errado"}

        return response

    def post(self):

        # abre conexão com banco de dados
        abastecimento = Abastecimentos()

        # processar o corpo recebido na solicitação
        data = request.json

        try:
            # insere o novo abastecimento nos dados recebidos
            response = abastecimento.insert_abastecimento(obj=data)
        except:
            response = {"status":"error","msg":"Algo deu errado"}

        return response


api.add_resource(Abastecimento,'/abastecimento/<string:name>/')
api.add_resource(ListAbastecimentos,'/abastecimento/')

if __name__ == '__main__':
    app.run(debug=True)