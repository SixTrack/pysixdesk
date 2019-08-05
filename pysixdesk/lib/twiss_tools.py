#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Author: David Siñuela Pastor <dsinuela@cern.ch>

import re


class TwissStruct:
    '''Structure containing Twiss file information'''
    APER_COLUMNS = ['APERTYPE', 'APER_1', 'APER_2', 'APER_3', 'APER_4']
    APER_VALUES = ['APER_1', 'APER_2', 'APER_3', 'APER_4']

    def __init__(self):
        self.globals = dict()
        self.element_fields = []
        self.elements = []

    def __str__(self):
        out = ""
        out += '* ' + " ".join(self.element_fields) + '\n'
        out += '$ ' + '\n'
        for element in self.elements:
            for kw in self.element_fields:
                out += element[kw] + ' '
            out = out[0:-1]  # remove space at the end
            out += '\n'
        return out

    def as_dict(self):
        return {element['NAME']: element for element in self.elements}


def split_line(line):
    '''Splits a line in fields, keeping quoted strings as a single field and
    striping out the quotes (double quotes only)'''
    return (string.strip('"') for string in re.findall(r'(?:")[^"]*(?:")|\S+',
                                                       line))


def read_twiss(file):
    '''Reads a Twiss file in a TwissStruct'''
    struct = TwissStruct()
    line = file.readline()
    while(line != ''):
        if line.startswith('@'):
            # Global variables
            tmps = split_line(line)
            line_chunks = []
            for tmp in tmps:
                line_chunks.append(tmp)
            prefix, kw, fmt, value, suffix = \
                line_chunks[0], line_chunks[1], line_chunks[2], \
                line_chunks[3], line_chunks[4:]
        elif line.startswith('*'):
            # Reading elements
            # Header with element fields
            line_chunks = line.split()
            prefix, struct.element_fields = line_chunks[0], line_chunks[1:]
            # Skip formats line
            file.readline()
            # Now read each element properties
            line = file.readline()
            while(line != ''):
                if line == "\n":  # skip empty lines
                    line = file.readline()
                    continue
                element_data = split_line(line)
                struct.elements.append(
                    dict(zip(struct.element_fields, element_data)))
                line = file.readline()
        line = file.readline()
    return struct


def read_icosim_csv(file):
    '''Reads an Icosim csv file in a TwissStruct'''
    _separator = ','
    struct = TwissStruct()
    line = file.readline().strip()
    while(line != ''):
        # Line starting with ALFX, ALFY contains the headers
        if line.startswith("ALFX,ALFY"):
            # Reading elements
            # Header with element fields
            struct.element_fields = line.split(_separator)
            # Now read each element properties
            line = file.readline().strip()
            while(line != ''):
                element_data = line.split(_separator)
                struct.elements.append(
                    dict(zip(struct.element_fields, element_data)))
                line = file.readline().strip()
        else:
            # Global variables
            line_chunks = line.split(_separator)
            kw, value, suffix = line_chunks[0], line_chunks[1], line_chunks[2:]
            struct.globals[kw] = value
        line = file.readline().strip()
    return struct


def empty_aperture(element):
    '''Returns true if the element doesn't have apertures defined'''
    zeros = True
    for kw in TwissStruct.APER_VALUES:
        try:
            if float(element[kw]) != 0e0:
                zeros = False
        except KeyError:
            pass
    return zeros


def compare_aperture(element1, element2, KEYS=TwissStruct.APER_VALUES):
    '''Returns true if the two elements have the same aperture values'''
    same = True
    for kw in KEYS:
        try:
            if float(element1[kw])-float(element2[kw]) != 0e0:
                same = False
        except KeyError:
            pass
    return same


def update_dict_entries(dest, origin):
    '''update dest entries with those in origin'''
    for kw in dest.keys():
        dest[kw] = origin[kw]
