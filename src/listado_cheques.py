from menu import Menu, Entry
from runtime import Runtime
from pandas import DataFrame, read_csv
from datetime import datetime


ERROR: int = -1
OK: int = 0
SUCCESS: int = 1


SALIDAS = ["PANTALLA", "CSV"]
ESTADOS = ["PENDIENTE", "APROBADO", "RECHAZADO", ""]
TIPOS = ["EMITIDO", "DEPOSITADO"]


# El orden de los argumentos son los siguientes:
#    a. Nombre del archivo csv.
#    b. DNI del cliente donde se filtraran.
#    c. Salida: PANTALLA o CSV
#    d. Tipo de cheque: EMITIDO o DEPOSITADO
#    e. Estado del cheque: PENDIENTE, APROBADO, RECHAZADO. (Opcional)
#    f. Rango fecha: xx-xx-xxxx:yy-yy-yyyy (Opcional)
def obtener_parametros():
    urlfile = input("\nIngrese el path del archivo: ")

    dni = int(input("Ingrese el DNI del usuario a consultar: "))

    salida = input("Elija si desea recibir la salida por PANTALLA o CSV: ")
    while salida not in (SALIDAS):
        salida = input("Elija si desea recibir la salida por PANTALLA o CSV: ")

    tipo = input(
        "Selecciones el tipo de cheque a buscar EMITIDO o DEPOSITADO: ")
    while tipo not in (TIPOS):
       tipo = input(
        "Selecciones el tipo de cheque a buscar EMITIDO o DEPOSITADO: ")     

    estado = input(
        "Selecciones el estado del cheque PENDIENTE, APROBADO, RECHAZADO (Opcional): ")
    while estado not in (ESTADOS):
        estado = input(
            "Selecciones el estado del cheque PENDIENTE, APROBADO, RECHAZADO (Opcional): ")

    rango = input(
        "Ingrese el rango de fechas en el formato xx-xx-xxxx:yy-yy-yyyy (Opcional): ")

    return urlfile, dni, salida, tipo, estado, rango


# Si el parámetro “Salida” es PANTALLA se deberá imprimir por pantalla todos
# los valores que se tienen, y si “Salida” es CSV se deberá exportar a un csv
# con las siguientes condiciones:
# a. El nombre de archivo tiene que tener el formato <DNI><TIMESTAMPS ACTUAL>.csv
# b. Se tiene que exportar las dos fechas, el valor del cheque y la cuenta
def exportar_cheques(dni: int, cheques: DataFrame, formato_de_salida: str):
    match formato_de_salida.upper():
        case "PANTALLA":
            print(cheques)

        case "CSV":
            timestamp = datetime.timestamp(datetime.now())
            cheques.to_csv(str(dni) + "_" + str(timestamp) +
                           ".csv", index=False)

        case _:
            raise("No se puede exportar los cheques según: " + formato_de_salida)


# Si para un DNI, dado un número de cheque de una misma cuenta de origen, se repite,
# se debe mostrar el error por pantalla, indicando que ese es el problema.
def verificar_cheques(cheques_del_usuario: DataFrame):
    cheques_por_cuenta = cheques_del_usuario.groupby("NumeroCuentaOrigen")

    for cuenta in cheques_por_cuenta.groups.keys():
        cheques = cheques_por_cuenta.get_group(cuenta)
        col_NroCheque = cheques["NroCheque"]
        if any(len(cheques[col_NroCheque == n]) > 1 for n in col_NroCheque.unique()):
            return ERROR

    return OK


# Si el estado del cheque no se pasa, se deberán imprimir
# los cheques sin filtrar por estado
def filtrar_por_estado(cheques: DataFrame, estado: str):
    if estado == "":
        return cheques
    else:
        return cheques[cheques["Estado"] == estado.upper()]


def obtener_cheques_del_usuario(dni: int, urlfile: str):
    cheques: DataFrame
    try:
        cheques = read_csv(urlfile)
    except Exception as error:
        print("Se produjo un error al intentar leer " + urlfile)
        print(error)

    return cheques[cheques["DNI"] == dni]


def consultar_cheques():
    urlfile, dni, formato_de_salida, _, estado, _ = obtener_parametros()

    cheques_del_usuario = obtener_cheques_del_usuario(dni, urlfile)

    if verificar_cheques(cheques_del_usuario) == OK:
        cheques_filtrados = filtrar_por_estado(cheques_del_usuario, estado)
        exportar_cheques(dni, cheques_filtrados, formato_de_salida)
        return SUCCESS

    else:
        print("\n[ERROR] Se encontraron números de cheques repetidos de una misma cuenta para el número de DNI del usuario.")
        return ERROR


###############################################################################

runtime: Runtime = Runtime()

options: list[Entry] = [
    Entry("Consultar cheques", lambda: consultar_cheques()),
    Entry("Salir", lambda: runtime.stop())
]

if __name__ == "__main__":
    runtime.start()

    menu: Menu = Menu()

    menu.load(options=options)

    print("Bienvenido al sistema de control de cheques.", end="\n")

    while runtime.status == runtime.RUNNING:

        print("\nPor favor seleccione una de las opciones:", end="\n")

        menu.render()

        print("Selección: ", end="")

        if menu.select() != ERROR:
            if menu.execute() != ERROR:
                continue

        runtime.stop()
