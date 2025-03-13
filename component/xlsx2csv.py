#!/usr/bin/env python3
# Executed with Python 3.4.10
import sys
import os
import time 
import zipfile
import csv

import xml.etree.ElementTree as ET

# Add the lib directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))

from data_process_lib import Component

class XLSXComponent(Component):

    def __init__(self):
        super().__init__()

        self.__namespace = {
            'main' : 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
            'r' : 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
            'relationship' : 'http://schemas.openxmlformats.org/package/2006/relationships'
        }

        self.__etree_tag_regex = ".//{{http://schemas.openxmlformats.org/spreadsheetml/2006/main}}{}"

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

        # Default Setting 
        config["seconds"] = config.get("seconds", 0)

        return config
        
    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")

        for filepath in self._data:
            self.__read_xlsx_content(filepath)

        self.log_info("End Process")

    def __get_sharedstrings(self, xml_tree):
        return [
            sharedstring.text 
            for sharedstring 
            in xml_tree.getroot().findall('main:si/main:t', self.__namespace)
        ]

    def __get_worksheets(self, xml_tree):
        return [
            (
                sheet.get("sheetId"), 
                sheet.get("name"), 
                sheet.get('{{{}}}id'.format(self.__namespace.get("r")))
            )
            for sheet 
            in xml_tree.getroot().findall('main:sheets/main:sheet', self.__namespace)
        ]

    def __get_relations(self, xml_tree):
        return {
            relation.get("Id") : relation.get("Target")
            for relation
            in xml_tree.getroot().findall('relationship:Relationship', self.__namespace)
        }
        
    def __read_worksheet(self, xml_tree, shared_strings):
        rows = xml_tree.findall(self.__etree_tag_regex.format("row"))

        data = []
        for row in rows:
            row_data = []
            for cell in row.findall(self.__etree_tag_regex.format("c")):
                cell_value = cell.find(self.__etree_tag_regex.format("v"))
                if cell_value is not None:
                    # Check if it is a shared string
                    if cell.get("t") == "s":
                        row_data.append(shared_strings[int(cell_value.text)])
                    else:
                        row_data.append(cell_value.text)
                else:
                    row_data.append("") 
            data.append(row_data)

        return data

    def __read_xlsx_content(self, filepath):
        file_basename, file_extension = os.path.splitext(os.path.basename(os.path.normpath(filepath)))

        with zipfile.ZipFile(filepath, "r") as zip_file:

            with zip_file.open('xl/sharedStrings.xml') as xml:
                shared_strings = self.__get_sharedstrings(ET.parse(xml))

            with zip_file.open("xl/workbook.xml") as xml:
                worksheets = self.__get_worksheets(ET.parse(xml))

            with zip_file.open("xl/_rels/workbook.xml.rels") as xml:
                relations = self.__get_relations(ET.parse(xml))
            
            for sheet_id, sheetname, sheet_rid in worksheets:
                with zip_file.open('xl/{}'.format(relations[sheet_rid])) as xml:
                    data = self.__read_worksheet(ET.parse(xml), shared_strings)
                    with open(os.path.join(self._OUTPUT_PATH, "{}_{}.csv".format(file_basename, sheetname)), "w") as fw:
                        csv.writer(fw).writerows(data)

if __name__ == "__main__":
    try:
        component = XLSXComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)