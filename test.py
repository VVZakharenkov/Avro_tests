import csv
import avro.schema
from avro.io import DatumReader, DatumWriter
from avro.datafile import DataFileReader, DataFileWriter

schema = avro.schema.parse(open("test.avsc", "rb").read())
writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)

with open('test.csv', newline='') as csvfile:
     reader = csv.DictReader(csvfile)
     for row in reader:
         record = {}
         for i in range(0, len(schema.fields)):
             record[schema.fields[i].name] = row[reader.fieldnames[i]]
         writer.append(record)
         ##writer.append({"USER_521": "row", "Last_Name": "row", "First_Name": "row"})
         ##print(row['521'], row['FN'], row['LN'])
writer.close()



