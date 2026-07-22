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

class CSV2XLSXComponent(AsyncComponent):

    __namespace = {
        'main' : 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
        'r' : 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
        'relationship' : 'http://schemas.openxmlformats.org/package/2006/relationships'
    }
    
    __xlsx_files = {
        'content_types' : """<?xml version="1.0" encoding="UTF-8"?>
            <Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
                <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
                <Default Extension="xml" ContentType="application/xml"/>
                <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
                <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
                <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
                <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
            </Types>
        """,
        'root_rels' : """<?xml version="1.0" encoding="UTF-8"?>
            <Relationships xmlns="{}">
                <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
            </Relationships>
        """.format(__namespace['relationship']),
        'workbook_rels' : """<?xml version="1.0" encoding="UTF-8"?>
            <Relationships xmlns="{}">
                <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
                <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
            </Relationships>
        """.format(__namespace['relationship']),
        'styles' : """<?xml version="1.0" encoding="UTF-8"?>
        <styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
            <numFmts count="0"/>
            <fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
            <fills count="1"><fill><patternFill patternType="none"/></fill></fills>
            <borders count="1"><border/></borders>
            <cellStyleXfs count="1"><xf/></cellStyleXfs>
            <cellXfs count="1"><xf/></cellXfs>
        </styleSheet>
        """,
        'app_xml' : """<?xml version="1.0" encoding="UTF-8"?>
            <Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties">
                <Application>Python</Application>
            </Properties>
        """
    }

    def __init__(self):
        super().__init__()

        ET.register_namespace("", self.__namespace['main'])
        ET.register_namespace("r", self.__namespace['r'])

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

        files = [ filepath for filepath in files if filepath.endswith(".csv")]
        if len(files) == 0:
            raise ImportError("No .csv files found")

        return files
        
    # Abstract from parent
    def _read_config(self, node_info):
        config = super()._read_config(node_info)

        # Default Setting 
        config["delimiter"] = config.get("delimiter", "|")
        
        return config
        
    # Abstract from parent
    def process(self):
        super().process()

        self.log_info("Start Process")
        
        futures = [ 
            self._executor.submit(self.write_xlsx_content, filepath, self._OUTPUT_PATH, self._config["delimiter"])
            for filepath
            in self._data
        ]
        
        for future in concurrent.futures.as_completed(futures):
            # Just to trigger exceptions if any
            future.result()

        self.log_info("End Process")
        
    @classmethod    
    def write_xlsx_content(cls, origin_filepath, destination_folder, delimiter):

        file_basename, file_extension = os.path.splitext(os.path.basename(os.path.normpath(origin_filepath)))

        cls.log_info("Reading csv[{}]".format("{}{}".format(file_basename, file_extension)))

        with open(origin_filepath, "r", newline="", encoding="utf-8") as csv_file:
            rows = list(csv.reader(csv_file, delimiter=delimiter))
        
        # worksheet.xml

        worksheet = ET.Element("{{{}}}worksheet".format(cls.__namespace['main']))
        sheet_data = ET.SubElement(worksheet, "{{{}}}sheetData".format(cls.__namespace['main']))

        for row_index, row in enumerate(rows, 1):
            row_element = ET.SubElement(sheet_data, "{{{}}}row".format(cls.__namespace['main']), { "r": str(row_index) })

            for col_index, value in enumerate(row, 1):
                cell_ref = "{}{}".format(cls._column_letter(col_index), row_index)
                cell = ET.SubElement(row_element, "{{{}}}c".format(cls.__namespace['main']), { "r": cell_ref, "t": "inlineStr" } )
                inline = ET.SubElement(cell, "{{{}}}is".format(cls.__namespace['main']))
                text = ET.SubElement(inline, "{{{}}}t".format(cls.__namespace['main']))
                text.text = value
                
        worksheet_xml = cls._xml_to_bytes(worksheet)
        
        # workbook.xml

        workbook = ET.Element("{{{}}}workbook".format(cls.__namespace['main']))
        sheets = ET.SubElement(workbook, "{{{}}}sheets".format(cls.__namespace['main']))
        ET.SubElement(sheets, "{{{}}}sheet".format(cls.__namespace['main']), { "name": "Sheet1", "sheetId": "1", "{{{}}}id".format(cls.__namespace['r']): "rId1" })
        workbook_xml = cls._xml_to_bytes(workbook)
        
        # Create XLSX

        with zipfile.ZipFile(os.path.join(destination_folder, "{}.xlsx".format(file_basename)), "w", zipfile.ZIP_DEFLATED) as xlsx:

            xlsx.writestr("[Content_Types].xml", cls.__xlsx_files['content_types'])
            xlsx.writestr("_rels/.rels", cls.__xlsx_files['root_rels'])
            xlsx.writestr("docProps/app.xml", cls.__xlsx_files['app_xml'])
            xlsx.writestr("xl/workbook.xml", workbook_xml)
            xlsx.writestr("xl/_rels/workbook.xml.rels", cls.__xlsx_files['workbook_rels'])
            xlsx.writestr("xl/styles.xml", cls.__xlsx_files['styles'])
            xlsx.writestr("xl/worksheets/sheet1.xml", worksheet_xml)
            
        cls.log_info("Created xlsx[{}]".format(file_basename))

    @staticmethod
    def _column_letter(number):

        result = ""

        while number > 0:
            number, remainder = divmod(number - 1, 26)
            result = chr(65 + remainder) + result

        return result
    
    @staticmethod
    def _xml_to_bytes(element):
        xml_header = b'<?xml version="1.0" encoding="utf-8"?>\n'
        return xml_header + ET.tostring(element, encoding="utf-8")

if __name__ == "__main__":
    try:
        component = CSV2XLSXComponent()
        component.init()
        component.process()
    except:
        component.log_exception("Exception Occured !!!")
        exit(1)
