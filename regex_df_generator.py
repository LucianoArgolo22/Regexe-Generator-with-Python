#%%
import pandas as pd
import numpy as np
from RegexGenerator import RegexGenerator
import json
import os
import re
import sys
from tabulate import tabulate

class Archivo:
    def __init__(self, folder, file_name, writing_path = None, dir = "regex_jsons"):
        self.folder = folder
        self.file_name = file_name
        self.writing_path = writing_path
        self.df = self.read_file()
        self.dir = dir
        self.columns_original = self.df.columns
        self.columns_regex = []
        self.columns_bool = []
        self.json_data_set = self.json_reader()
        self.df_json_data_set = pd.DataFrame(self.json_data_set) if self.json_data_set else None


    def json_reader(self):
        #probar de ver si se puede cambiar por un if
        try:
            with open (f"{self.dir}/final_{self.file_name.split('.')[0]}_REGEX.json", 'r') as jsonFile:
                return json.load(jsonFile)
        except:
            return None

    def read_file(self, sep = ";", dtype = 'str'):
        encodings = ['utf-8', 'ISO-8859-1']  # None is default
        for encoding in encodings:
            try:
                df = pd.read_csv(f"{self.folder}/{self.file_name}", sep=sep, dtype = dtype, encoding = encoding)
                df.index = range(2,len(df)+2)
                return df
            except Exception:  # should really be more specific 
                print(encoding)
        raise ValueError("{} is has no encoding in {}".format(self.file_name, encodings))        

    def mkdir_if_not_exists(self):
        if self.dir not in os.listdir():
            os.mkdir(self.dir)

    @staticmethod
    def dict_gen(df_):
        json_dict = {}
        [json_dict.update({key : list(df_[key])}) for key in df_.keys()]
        return json_dict

    def write_file(self,mode='w'):
        self.mkdir_if_not_exists()
        with open(f'{self.dir}/final_{self.file_name.split(".")[0]}_REGEX.json', mode) as f:
            json.dump(self.return_processed_file(), f, indent = 3)

    def file_line_headers(self):
        encodings = ['utf-8', 'ISO-8859-1']  # None is default
        for encoding in encodings:
            try:
                with open(f"{self.folder}/{self.file_name}", 'r', encoding=encoding) as file:
                    line = file.readlines()
                    headers_ = line.pop(0).split(";")
                    return line, headers_
            except Exception:  # should really be more specific 
                print(encoding)
        raise ValueError("{} is has no encoding in {}".format(self.file_name, encodings))   

class RegexFunctions(Archivo):
    def number_validation(self, column):
        self.df[column + "_Validated"] = self.df[column].str.contains('\d+')

    def apply_op(self, row):
        return "nan" if str(row) == "nan" else RegexGenerator(str(row)).get_regex()

    def apply_op_2(self, row, column):
        return row in self.json_data_set[column + "_Regex"]

    def column_regex(self, column):
        self.columns_regex.append(column + "_Regex")
        self.df[column + "_Regex"] = self.df[column].apply(lambda x: self.apply_op(x))

    def column_regex_Bool(self, column):
        self.columns_bool.append(column + "_Ok")
        self.df[column + "_Ok"] = self.df[column + "_Regex"].apply(lambda x: self.apply_op_2(x,column))
 

    def get_unique_values(self, column):
        return np.array(self.df[column].unique(), dtype=str)

    def dict_unique_values_generator(self, df_dict):
        [df_dict.update({column : self.get_unique_values(column)}) for column in self.columns_regex] 


    def dataframe_regexs(self, Bool = False):
        if Bool:
            [self.column_regex_Bool(column) for column in self.columns_original if "_Regex" not in column]
        else:
            [self.column_regex(column) for column in self.columns_original]

    def process_file(self):
        return self.distinct_regexs()

    def return_processed_file(self):
        return self.dict_gen(df_ = self.process_file())

    def distinct_regexs(self):
        df_dict = {}
        self.dataframe_regexs()
        self.dict_unique_values_generator(df_dict)
        regex_df = pd.DataFrame.from_dict(df_dict, orient = 'index')
        return regex_df.transpose()

    def bool_df(self):
        self.json_reader()
        self.dataframe_regexs()
        self.dataframe_regexs(Bool=True)
        self.finding_values_to_replace()

    def boolean_mask(self):
        #Máscara boolena para traer solo las columnas con algún campo False       
        return self.df[self.columns_bool].sum(axis=1) != len(self.columns_bool)

    def columns_final(self):
        columnas_final = []
        for i in range(len(self.columns_original)):
            columnas_final.append(self.columns_original[i])
            columnas_final.append(self.columns_bool[i])
        return columnas_final
    
    def df_result(self):
        columnas_final, errors = self.columns_final(), self.boolean_mask()
        return  self.df[columnas_final][errors].replace({True: "Ok", False: "CORREGIR"})
    
    def generating_errors_file(self):
        self.bool_df()
        resultado_df = self.df_result()
        if resultado_df.shape[0] != 0:
            print(tabulate(resultado_df, headers = 'keys', tablefmt = 'pretty'))
            os.mkdir("errors") if "errors" not in os.listdir() else None            
            resultado_df.to_csv(f"errors/{self.file_name.split('.')[0]}_errores.csv", sep ="|", index = True)
        
        else:
            print("Todos los datos ingresados estan correctos")
            archivo = self.file_name.split(".")[0] + "_errores.csv"
            if archivo in os.listdir(errors_folder):
                os.remove(errors_folder + "/" + archivo)

class RegexFuncExtended(RegexFunctions):
    def regex_union_dict(self):
        df_regex = self.distinct_regexs()
        dict_regex = {}
        for columna in list(df_regex.columns):
            self.updating_dict(df_regex, columna, dict_regex)
        final_df = pd.DataFrame.from_dict(dict_regex, orient = 'index')
        return final_df.transpose()

    def process_file(self):
        return self.regex_union_dict()

    def updating_dict(self, df_regex, columna, dict_regex):
        set_regex, set_json = self.dropping_all_na(df_regex, columna)
        new_regex_set = list(set_regex.union(set_json))
        dict_regex.update(dict(zip([columna],[new_regex_set])))

    def dropping_all_na(self, df_regex, columna):
        set_regex = self.drop_na(df_regex, columna)
        set_json = self.drop_na(self.df_json_data_set, columna)
        return set_regex, set_json

    def drop_na(self, df, columna):
        return set(df[columna].dropna())

    def finding_values_to_replace(self, values_to_find = '["]'):
        line, headers_ = self.file_line_headers()
        for row, lin in enumerate(line):
            valor = re.findall(f'{values_to_find}', lin)
            if valor:
                for position, value in enumerate(lin.split(";")):
                    field = re.findall(f'{values_to_find}', value)
                    if field:
                        self.replacing_boolean_matrix(headers_, position, row)

    def replacing_boolean_matrix(self, headers_, position, row):
        self.df[headers_[position]+"_Ok"][row+2] = False
        self.df[headers_[position]][row+2] = "'" + self.df[headers_[position]][row+2] + "'"



if "__main__" == __name__:
    csv_folder = "Input"
    errors_folder = "errors"

    camino = True if str(sys.argv[1]) == "True" else False
    if camino:
                #%%
        # --------1
        #Genera un JSON con las regex para todos los archivos en la carpeta Input
        #que no tengan un JSON de regex ya generado en la carpeta "regex_jsons".
        #Chequear que el nombre del archivo en Input coincida con su equivalente en "regex_json", 
        #sin el prefijo final ni el sufijo REGEX. Ej: en input pongo tacticos_mitre.csv, en regex_json debe
        #existir final_tactico_mitre_REGEX.json. Si no existe, lo creará.
        # --------2
        #Además se evalua si los campos de las tablas en input coinciden con sus respetivas regex.
        #Los casos que no coincidan se muestran en pantalla, y se genera un log de errores en la carpeta "errors".
        #Cuando los errroes se subsanen, el log generado se borra.
        #Si se considera que el archivo esta bien, aún con los errores, es necesario agregar la nueva regex a su JSON.
        #Para esto leer la segunda parte.
        for archivo in os.listdir(csv_folder): 

            if ".csv" in archivo:
                    regex_jsons = [file.split("_REGEX")[0] for file in os.listdir("regex_jsons")]
                    if "final_" + archivo.split(".")[0] not in regex_jsons:
                        archivito = RegexFunctions(csv_folder, archivo)
                        archivito.write_file()

            archivito = RegexFuncExtended(csv_folder,archivo)
            archivito.generating_errors_file()

    else:
        # --------1
        #Genera un JSON con las regex para todos los archivos en la carpeta Input
        #que no tengan un JSON de regex ya generado en la carpeta "regex_jsons".
        #Chequear que el nombre del archivo en Input coincida con su equivalente en "regex_json", 
        #sin el prefijo final ni el sufijo REGEX. Ej: en input pongo tacticos_mitre.csv, en regex_json debe
        #existir final_tactico_mitre_REGEX.json. Si no existe, lo creará.
        # --------2
        #Agrega más regex al dataset de regex en caso que haya campos
        #que no fueron considerado como válidos, pero que realmente lo eran.
        #Si se corre sin que haya un json de regex previo, se rompe.
        #IMPORTANTE, NO CORRER CON UN ARCHIVO DENTRO DE LA CARPETA INPUT QUE NO SEA CORRECTO
        #SINO EL DATASET DE VALIDACION SE HECHA A PERDER
        for archivo in os.listdir(csv_folder):
            print(archivo)
            if ".csv" in archivo:
                regex_jsons = [file.split("_REGEX")[0] for file in os.listdir("regex_jsons")]

                if archivo.split(".")[0] not in regex_jsons:
                    archivito = RegexFunctions(csv_folder, archivo)
                    archivito.write_file()

            archivito = RegexFuncExtended(csv_folder,archivo)
            df = archivito.write_file()
