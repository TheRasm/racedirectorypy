import time, datetime, csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from time import gmtime, strftime
import pandas as pd

path_main = "C:\\Users\\renes\\OneDrive\\Desktop\\TimeTrack\\R&D\\directortest\\"

#direccion de archivos base 
bautizo_path = path_main + 'archivos_base\\bautizo.csv'
informacion_path = path_main + 'archivos_base\\info.csv'

#lecturas 
llegada_limpia = path_main+"lecturas\\llegadas_limpias.txt" #texto limpio: chip, tiempo
llegada_path = path_main+"lecturas\\llegadas.txt"

#archivos secundarios
llegadas_limpias = path_main+"secundarios\\llegadas_limpias.txt" #texto limpio: chip, tiempo
testito_path = path_main +"secundarios\\testito.txt"
llegada_limpia_csv = path_main+"secundarios\\llegada_limpia.csv" #csv ordenado
llegada_nodup_csv = path_main+"secundarios\\llegada_nodup.csv" #csv sin duplicados
llegada_secuencia = path_main+"secundarios\\llegada_secuencia.csv"#csv sin duplicados y en secuencia

#resultados
resultados1= path_main+"resultados\\resultados1.csv" #numero y tiempo
resultados2=path_main+"resultados\\resultados2.txt" #numero y tiempo
res = path_main+"resultados\\res.csv" #numero y tiempo
resfin = path_main+"resultados\\resfin.csv"

#hora de salida
hora = "10:00:00.000"
hora_disparo = datetime.timedelta(hours=10) #add my starting hour, or gun time


#inicio api 1
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name('pepe.json', scope)
client = gspread.authorize(credentials)
spreadsheet = client.open('resultadostest')

######incluyo diccionario de chips #######################
with open(bautizo_path, mode='r') as infile:
    reader = csv.reader(infile)
    chip_ID = {rows[1]:rows[0] for rows in reader}

######incluyo diccionario de participantes######## nested dictionary
f = open(informacion_path, 'r')
first_line = f.readline()
reader = csv.reader(f)  # omite encabezado
participantes = {} # genero lista llamada aretes, es una lista comuesta de listas
for row in reader:
    participantes[row[0]] = {'NAME': row[1],'LAST NAME': row[2],'DIV': row[3],
                            'GENDER':row[4],'DATE OF BIRTH':row[5],'AGE':row[6],
                            'TEAM': row[7],'EMAIL': row[8],'CITY': row[9],'COUNTRY': row[10],
                            'TYPE OF BIKE': row[11],'PARTICIPA': row[12],'FECHA REGISTRO': row[13]}

#print(participantes[str(2)]['NAME']) #busca el nombre del numero 2

#incluyo viejas lecturas directo de mylaps 
def formatear(lecturas_mylaps, archivo_resultados):
    old_file = open(lecturas_mylaps, "r")
    lines = old_file.read().split("\n")
    new_lines= []
    for line in lines:
        newline = line[:7] +',' + line[7:18]   # Add more indexes here
        new_lines.append(newline)
    old_file.close()

    #genero nuevo archivo con el nuevo formato 
    new_file = open(llegadas_limpias, "w")
    new_file.write('chip,tiempo'+"\n")
    with new_file:    
        new_file.write("\n".join(new_lines))
    new_file.close()

    # genero nuevo archivo usando pandas "buena llegada"
    text_file = pd.read_csv(llegadas_limpias, delimiter = ',')
    text_file.to_csv(llegada_limpia_csv, index=False)

    #Eliminino duplicados "no duplicados"
    df = pd.read_csv(llegada_limpia_csv, usecols=['chip', 'tiempo']).drop_duplicates(subset = 'chip', keep= 'first')
    df.to_csv(llegada_nodup_csv, index= False)

    #Acomodo por tiempo "orden"
    pd.read_csv(llegada_nodup_csv, usecols=['chip', 'tiempo']).sort_values(by='tiempo', ascending=True).to_csv(llegada_secuencia, index= False)

    #comparo llegada con diccionaripo de chip y genero resultados en archivo de texto
    with open(resultados2, "w") as my_output:
        with open(llegada_secuencia, "r") as my_input_file:
            next(my_input_file, None) #skip header
            my_output.write("Lugar, Numero, Tiempo" + "\n")
            lugar=1
            for row in csv.reader(my_input_file):
                    #print(str(str(chip_ID.get(str(row[0]))))+","+str(row[1]))
                    my_output.write(str(str(lugar)+","+str(chip_ID.get(str(row[0])))+","+str(row[1])+"\n"))
                    lugar+=1

    # #######################################################

    # genero nuevo archivo csv a partir de archivo de texto sin formato
    text_file = pd.read_csv(resultados2, delimiter = ',')
    text_file.to_csv(archivo_resultados, index=False)

    #resto la hora de salida 

def ajusta_llegadas(archivo_in, archivo_out, disparo):
    with open(archivo_in, 'r') as in_file:
        csv_reader = csv.reader(in_file)
        next(csv_reader, None) #skip header
        # open the output file and create a CSV writer object
        with open(archivo_out, 'w', newline='') as out_file:
            csv_writer = csv.writer(out_file)
            csv_writer.writerow(["Lugar", "Numero", "Tiempo"])
            # iterate over the rows in the input file
            for row in csv_reader:
                # remove the last 3 characters from the third column
                row[2] = row[2][:-3]
                date_string = str(row[2])
                date_object = datetime.datetime.strptime(date_string,"%H:%M:%S")
                # rest 10 hours to datetime object
                date_object -= datetime.timedelta(hours=10) #add my starting hour, or gun time
                #date_object -= disparo #add my starting hour, or gun time

                # Convert datetime object back to string
                new_date_string = date_object.strftime("%H:%M:%S")
                row[2] = new_date_string
                # write the modified row0 to the output file
                csv_writer.writerow(row)

def publicar_resultados(archivo):
    with open(archivo, 'r') as file_obj:
        content = file_obj.read()
        client.import_csv(spreadsheet.id, data=content)


if __name__ == '__main__':
    formatear(llegada_path,res)
    ajusta_llegadas(res,resfin, hora_disparo)
    publicar_resultados(resfin)
     

     

