import re
import xlrd

from tap_s3_csv.logger import LOGGER as logger


def generator_wrapper(reader):
    to_return = {}

    header_row = None

    for row in reader:
        if header_row is None:
            header_row = row
            continue

        for index, cell in enumerate(row):
            header_cell = header_row[index]

            formatted_key = header_cell.value

            # remove non-word, non-whitespace characters
            formatted_key = re.sub(r"[^\w\s]", '', formatted_key)

            # replace whitespace with underscores
            formatted_key = re.sub(r"\s+", '_', formatted_key)

            to_return[formatted_key.lower()] = cell.value

        yield to_return


def get_row_iterator(table_spec, file_handle):
    workbook = xlrd.open_workbook(
        on_demand=True,
        file_contents=file_handle.read())

    sheet = workbook.sheet_by_name(table_spec["worksheet_name"])
    reader = sheet.get_rows()
    if 'skip_lines' in table_spec:
        logger.info(f"skipping {table_spec['skip_lines']} line(s) from beginning of file")
        for i in range(table_spec['skip_lines']):
            next(reader)

    return generator_wrapper(reader)
