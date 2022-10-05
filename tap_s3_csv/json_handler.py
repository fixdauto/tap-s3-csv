import codecs
from io import TextIOWrapper
import json
def get_row_iterator(table_spec,file_handle):
    file_stream = codecs.iterdecode(file_handle._raw_stream, encoding='utf-8')
    for line in file_handle._raw_stream.readlines():
        json_line = json.loads(line)
        outer_keys = json_line.keys()
        if 'subject' in outer_keys:
            keys = list(json_line['subject'].keys())
            yield json_line['subject'][keys[0]]
        else:
            yield json_line
