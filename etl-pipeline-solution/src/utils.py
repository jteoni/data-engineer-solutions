import json
import gzip

def compress_json(input_file, output_file):
    with gzip.open(input_file, 'rt', encoding='utf-8') as f_in:
        data = json.load(f_in)  # Carrega o JSON comprimido para um objeto Python
    
    with gzip.open(output_file, 'wt', encoding='utf-8') as f_out:
        json.dump(data, f_out)  # Escreve o JSON comprimido no arquivo de saída
    
    print(f'Arquivo comprimido criado: {output_file}')

input_filename = 'data/data-nyctaxi-trips-2009.json.gz'
output_filename = 'data/data-nyctaxi-trips-2009_compressed.json.gz'

compress_json(input_filename, output_filename)
