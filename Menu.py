import time
def print_menu():
    print("""-------- Menu --------
1.- Opcion 1
2.- Opcion 2
3.- Opcion 3
4.- Opcion 4
5.- Exit""")

def main():
    while True:
        try:
            print_menu()
            option = int(input("Seleccione una opcion:"))

            if option == 1:
                print("OPTION 1")
                time.sleep(1)
            elif option == 2:
                print("OPTION 2")
                time.sleep(1)
            elif option == 3:
                print("OPTION 3")
                time.sleep(1)
            elif option == 4:
                print("OPTION 4")
                time.sleep(1)
            elif option == 5:
                print("Gracias por usar el programa.")
                time.sleep(1)
                break
            else: 
                print("Error: Ingrese un numero valido.")
                time.sleep(1)
        except ValueError:
            print("Error: Ingrese un numero.")
        time.sleep(1)

if __name__ == '__main__':
    main()
