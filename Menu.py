import time
import Poblation
import ModelCassandra
import Consultas
import ModelDgraph
from cassandra.cluster import Cluster

def print_menu():
    print("""\n########## Menú Principal ##########
1.- Cassandra
2.- MongoDB
3.- Dgraph
4.- Salir""")

def main():
    while True:
        try:
            print_menu()
            option = int(input("Seleccione una opción: "))

            if option == 1:
                print("\nHas seleccionado Cassandra.")
                while True:
                    ModelCassandra.print_menuCassandra()
                    try:
                        optionCass = int(input("Seleccione una opción de Cassandra: "))
                        if optionCass == 1:
                            print("Creando keyspace y tablas de Cassandra...")
                            session = ModelCassandra.connect()
                            ModelCassandra.create_keyspace_and_table(session)
                            time.sleep(1)
                        elif optionCass == 2:
                            print("Poblando tablas de Cassandra...")
                            Poblation.poblar_tablas()
                            time.sleep(1)
                        elif optionCass == 3:
                            print("Querys")
                            session = Consultas.connect()
                            Consultas.menu_querys(session)
                        elif optionCass == 4:
                            print("Eliminar datos en tablas...")
                            session = ModelCassandra.connect()
                            ModelCassandra.drop_all_tables(session)
                            time.sleep(1)
                        elif optionCass == 5:
                            print("Regresando al menú principal...")
                            break
                        else:
                            print("Error: Ingrese un número válido.")
                            time.sleep(1)
                    except ValueError:
                        print("Error: Ingrese un número válido.")
                        time.sleep(1)

            elif option == 2:
                print("Has seleccionado MongoDB.")
                time.sleep(1)
            elif option == 3:
                print("Has seleccionado Dgraph.")
                client_stub = ModelDgraph.pydgraph.DgraphClientStub('localhost:9080')
                client = ModelDgraph.pydgraph.DgraphClient(client_stub)
                ModelDgraph.set_schema(client)
                ModelDgraph.menu_dgraph(client)
            elif option == 4:
                print("Gracias por usar el programa.")
                time.sleep(1)
                break
            else:
                print("Error: Ingrese un número válido.")
        except ValueError:
            print("Error: Ingrese un número válido.")
        time.sleep(1)

if __name__ == '__main__':
    main()
