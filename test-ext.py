import csv
import avro.schema
from avro.io import DatumReader, DatumWriter
from avro.datafile import DataFileReader, DataFileWriter

schema = avro.schema.parse(open("test-ext.avsc", "rb").read())
writer = DataFileWriter(open("users-ext.avro", "wb"), DatumWriter(), schema)

arr =[123232, 2232323, 3232323, 114232323]
record = {"USER_521": "User2", "Last_Name": "LN_1", "First_Name": "FN_1", "Attributes": arr}
writer.append(record)
writer.close()



