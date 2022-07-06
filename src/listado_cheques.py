#!/usr/bin/python3


"""
 Copyright (c) 2022 Savinelli Roberto Nicolás <savinelli@ieee.org>
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
"""


from argparse import ArgumentParser, Namespace
from datetime import datetime
from types import NoneType
from typing import Callable
from pandas import DataFrame, read_csv
import logging


ERROR: int = -1
OK: int = 0
SUCCESS: int = 1


OUTPUT_FORMATS: list[str] = ["PANTALLA", "CSV"]
STATES: list[str] = ["PENDIENTE", "APROBADO", "RECHAZADO"]
TYPES: list[str] = ["EMITIDO", "DEPOSITADO"]


###############################################################################


def load_csv(file_url: str) -> DataFrame:
    df: DataFrame

    try:
        df = read_csv(file_url)

    except Exception as error:
        raise (error)

    return df


def store_csv(file_url: str, df: DataFrame) -> int:
    try:
        df.to_csv(file_url, index=False)

    except Exception as error:
        raise (error)

    return SUCCESS


def filter_df(
    df: DataFrame, filters: list[Callable[[DataFrame], DataFrame]]
) -> DataFrame:
    filtered_df: DataFrame = df.copy()

    for filter in filters:
        filtered_df = filter(filtered_df)

    return filtered_df


###############################################################################


# Si el parámetro “Salida” es PANTALLA se deberá imprimir por pantalla todos
# los valores que se tienen, y si “Salida” es CSV se deberá exportar a un csv
# con las siguientes condiciones:
# a. El nombre de archivo tiene que tener el formato <DNI><TIMESTAMPS ACTUAL>.csv
# b. Se tiene que exportar las dos fechas, el valor del cheque y la cuenta
def _exportar_cheques(dni: int, cheques: DataFrame, formato: str) -> None:
    match formato.upper():
        case "PANTALLA":
            print(cheques)

        case "CSV":
            timestamp: str = str(datetime.timestamp(datetime.now()))
            file_name = "{}_{}.csv".format(str(dni), timestamp)
            store_csv(file_name, cheques)

        case _:
            raise ("No se puede exportar los cheques según: " + formato)


# Si para un DNI, dado un número de cheque de una misma cuenta de origen, se repite,
# se debe mostrar el error por pantalla, indicando que ese es el problema.
def _verificar_cheques(cheques_del_usuario: DataFrame) -> int:
    cheques_por_cuenta = cheques_del_usuario.groupby("NumeroCuentaOrigen")

    for cuenta in cheques_por_cuenta.groups.keys():
        cheques = cheques_por_cuenta.get_group(cuenta)
        col_NroCheque = cheques["NroCheque"]
        if any(len(cheques[col_NroCheque == n]) > 1 for n in col_NroCheque.unique()):
            return ERROR

    return OK


# Si el estado del cheque no se pasa, se deberán imprimir
# los cheques sin filtrar por estado
def _filtrar_por_estado(cheques: DataFrame, estado: str | NoneType) -> DataFrame:
    if isinstance(estado, str):
        return cheques[cheques["Estado"] == estado.upper()]
    else:
        return cheques


def _filtrar_por_tipo(cheques: DataFrame, tipo: str | NoneType) -> DataFrame:
    if isinstance(tipo, str):
        return cheques[cheques["Tipo"] == tipo.upper()]
    else:
        return cheques


def _filtrar_por_dni(cheques: DataFrame, dni: int | NoneType) -> DataFrame:
    if isinstance(dni, int):
        return cheques[cheques["DNI"] == dni]
    else:
        return cheques


def run(
    file_url: str,
    dni: int,
    formato: str,
    tipo: str,
    estado: str | NoneType,
    rango: str | NoneType,
) -> int:
    try:
        cheques: DataFrame = load_csv(file_url)
    except Exception as error:
        logging.error(
            "Se produjo una falla al intentar leer el archivo {}\n{}".format(
                file_url, str(error)
            )
        )
        return ERROR

    cheques_del_usuario: DataFrame = _filtrar_por_dni(cheques, dni)

    if _verificar_cheques(cheques_del_usuario) == OK:
        filtros: list[Callable[[DataFrame], DataFrame]] = [
            lambda df: _filtrar_por_estado(df, estado),
            lambda df: _filtrar_por_tipo(df, tipo),
        ]

        cheques_filtrados: DataFrame = filter_df(cheques_del_usuario, filtros)

        try:
            _exportar_cheques(dni, cheques_filtrados, formato)
        except Exception as error:
            logging.error(
                "Se produjo una falla al exportar los cheques\n{}".format(str(error))
            )
            return ERROR

        return SUCCESS

    else:
        logging.error(
            "Se encontraron números de cheques repetidos de una misma cuenta para el número de DNI del usuario."
        )
        return ERROR


###############################################################################


# El orden de los argumentos son los siguientes:
#    a. Nombre del archivo csv.
#    b. DNI del cliente donde se filtraran.
#    c. Salida: PANTALLA o CSV
#    d. Tipo de cheque: EMITIDO o DEPOSITADO
#    e. Estado del cheque: PENDIENTE, APROBADO, RECHAZADO. (Opcional)
#    f. Rango fecha: xx-xx-xxxx:yy-yy-yyyy (Opcional)


parser: ArgumentParser = ArgumentParser(description="Procesamiento Batch de cheques")


parser.add_argument("-d", "--dni", help="DNI del cliente", type=int, required=True)
parser.add_argument(
    "-t", "--type", help="Tipo de cheque", choices=TYPES, type=str, required=True
)
parser.add_argument(
    "-s", "--state", help="Estado del cheque", choices=STATES, type=str, required=False
)
parser.add_argument(
    "-i",
    "--input-file",
    help="Path al archivo csv con los cheques",
    type=str,
    required=True,
)
parser.add_argument(
    "-f",
    "--output-format",
    help="Formato del output del programa",
    choices=OUTPUT_FORMATS,
    type=str,
    required=True,
)
parser.add_argument(
    "-r",
    "--range",
    help="Rango de fecha del cheque en formato XX-XX-XXXX:YY-YY-YYYY",
    type=str,
    required=False,
)


###############################################################################


if __name__ == "__main__":
    args: Namespace = parser.parse_args()

    exit(
        run(
            args.input_file,
            args.dni,
            args.output_format,
            args.type,
            args.state,
            args.range,
        )
    )
