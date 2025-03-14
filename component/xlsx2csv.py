#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time 
import zipfile
import csv
import string

import xml.etree.ElementTree as ET
import concurrent.futures

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from data_process_lib import AsyncComponent

def column_letter_to_index(col_letter):
    return sum(
        (string.ascii_uppercase.index(character) + 1) * (26 ** index)
        for index, character
        in enumerate(reversed(col_letter))
    )

def get_sharedstrings(namespace, xml_tree):
    return [
        sharedstring.text 
        for sharedstring 
        in xml_tree.getroot().findall('main:si/main:t', namespace)
    ]

def get_worksheets(namespace, xml_tree):
    return [
        (
            sheet.get("sheetId"), 
            sheet.get("name"), 
            sheet.get('{{{}}}id'.format(namespace.get("r")))
        )
        for sheet 
        in xml_tree.getroot().findall('main:sheets/main:sheet', namespace)
    ]

def get_relations(namespace, xml_tree):
    return {
        relation.get("Id") : relation.get("Target")
        for relation
        in xml_tree.getroot().findall('relationship:Relationship', namespace)
    }
    
def read_worksheet(namespace, xml_tree, shared_strings):
    max_row = 0
    max_col = 0
    data = []

    for cell in xml_tree.getroot().findall("main:sheetData/main:row/main:c", namespace):
        cell_reference = cell.get("r")
        cell_row = int(''.join(filter(str.isdigit, cell_reference)))
        cell_col = int(column_letter_to_index(''.join(filter(str.isalpha, cell_reference))))
        
        max_row = max(max_row, cell_row)
        max_col = max(max_col, cell_col)

        cell_value = cell.find("main:v", namespace)
        cell_text = None
        if cell_value is not None:
            cell_text = shared_strings[int(cell_value.text)] if cell.get("t") == "s" else cell_value.text
        
        # return value, index row, index col --> starts from 0 so -1
        data.append((cell_text, cell_row - 1, cell_col - 1))

    return data, max_row, max_col

def read_xlsx_content(origin_filepath, destination_folder):

    namespace = {
        'main' : 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
        'r' : 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
        'relationship' : 'http://schemas.openxmlformats.org/package/2006/relationships'
    }

    file_basename, file_extension = os.path.splitext(os.path.basename(os.path.normpath(origin_filepath)))

    with zipfile.ZipFile(origin_filepath, "r") as zip_file:

        with zip_file.open('xl/sharedStrings.xml') as xml:
            shared_strings = get_sharedstrings(namespace, ET.parse(xml))

        with zip_file.open("xl/workbook.xml") as xml:
            worksheets = get_worksheets(namespace, ET.parse(xml))

        with zip_file.open("xl/_rels/workbook.xml.rels") as xml:
            relations = get_relations(namespace, ET.parse(xml))
        
        for sheet_id, sheetname, sheet_rid in worksheets:
            with zip_file.open('xl/{}'.format(relations[sheet_rid])) as xml:
                data, num_rows, num_cols = read_worksheet(namespace, ET.parse(xml), shared_strings)
                matrix = [[None] * num_cols for _ in range(num_rows)]
                
                for value, index_row, index_col in data:
                    matrix[index_row][index_col] = value

                with open(os.path.join(destination_folder, "{}_{}.csv".format(file_basename, sheetname)), "w") as fw:
                    csv.writer(fw).writerows(matrix)


class XLSX2CSVComponent(AsyncComponent):

    def __init__(self):
        super().__init__()

    # Abstract from parent
    def _read_input(self, input_list):
        files = []

        for input_record in input_list:
            if os.path.isdir(input_record):
                files = files + [ os.path.join(root, filename) for root, dirs, files in os.walk(input_record) if len(files) > 0 for filename in files ]
            elif os.path.isfile(input_record):
                files.append(input_record)
            else:
                raise ImportError("Path {} is incorrect".format(input_record))

        files = [ filepath for filepath in files if filepath.endswith(".xlsx")]
        if len(files) == 0:
            raise ImportError("No xlsx files found")

        return files
        
    # Abstract from parent
    def _read_config(self, node_info):
        config = super()._read_config(node_info)
        return config
        
    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")

        futures = [ 
            self._executor.submit(read_xlsx_content, filepath, self._OUTPUT_PATH)
            for filepath
            in self._data
        ]
    
        for future in concurrent.futures.as_completed(futures):
            # Just to trigger exceptions if any
            future.result()

        self.log_info("End Process")

if __name__ == "__main__":
    try:
        component = XLSX2CSVComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)