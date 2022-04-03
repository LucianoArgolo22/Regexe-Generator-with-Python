#%%
"""
Recursos:
Librería Python para generar Regex: https://pypi.org/project/regex-generator-lib/

Página que genera Regex ingresando strings manualmente: https://regex-generator.olafneumann.org/?sampleText=2020-03-12T13%3A34%3A56.123Z%20INFO%20%20%5Borg.example.Class%5D%3A%20This%20is%20a%20%23simple%20%23logline%20containing%20a%20%27value%27.&flags=i&onlyPatterns=false&matchWholeLine=false&selection=

Página para validación de Regex: https://regex101.com/
"""



#%%
#Ejemplo con librería re de Python
import re 

lista = ["hola","12","caramelos"]

pattern = '[a-z]+'

for valor in lista:
    print(re.findall(pattern, valor))



#%%
#Ejemplo con RegexGenerator
def regex(string):
        return RegexGenerator((str(string).strip())).get_regex()

def abrir_archivo(file):
    with open(file, "r") as file:
        return file.readlines()


if __name__ == "__main__":

    datos_por_validar = abrir_archivo("datos.txt")
    schema_permitido = abrir_archivo("schema_datos_permitidos.txt")

    regex_validas = []

    for dato in schema_permitido:
        regex_validas.append(regex(dato))

    #%%
    regex_validas

    #%%
    datos_validados = []
    for dato_v in datos_por_validar:
        if regex(dato_v) in regex_validas:
            datos_validados.append(dato_v)
            
    #%%

    with open("datos_validados.txt", "w") as file:
        file.write("\n".join(datos_validados))

    #%%
#Ejemplo con RegexGenerator y Pandas
import pandas as pd
if __name__ == "__main__":
    df_datos_a_validar = pd.read_csv("datos.txt")


    for campo in df_datos_a_validar.columns:
        df_datos_a_validar[campo + "_REGEX"] = [regex(df_datos_a_validar[campo][i]) for i in range(len(df_datos_a_validar))]

    
    df_datos_a_validar




