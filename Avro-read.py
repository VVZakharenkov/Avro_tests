import avro.schema
import json
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

reader = DataFileReader(open("users.avro", "rb"), DatumReader())
schema = reader.datum_reader.writer_schema
print(schema)
dict = json.loads(reader.meta.get('avro.schema').decode('utf-8'))
for user in reader:
    for k, v in user.items():
        print(k, v)
reader.close()