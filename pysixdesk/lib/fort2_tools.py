#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Author: Pablo Garcia Ortega <pgarciao@cern.ch>

# Warning: the reading of the file is not by any means complete, only reads the
# elements at the top of the file

from . import utils
from copy import deepcopy

FORT2_ELEMENT_FIELDS = [
    'NAME',
    'TYPE',
    'VAR1',
    'VAR2',
    'LENG',
    'VAR4',
    'VAR5',
    'VAR6',
]
FORT2_BLOCK_FIELDS = [
    'NAME',
    'ELEM'
]

LOGGER = utils.condor_logger("fort2_tools")


class Fort2Struct:
    '''Structure containing fort.2 information'''

    def __init__(self):
        self.elements = []
        self.blocks = []
        self.lattice = []

    def echoDimensions(self):
        '''return dimensions of arrays (ie SINGLE ELEMENTs, BLOCs,
        LATTICE ELEMENTs)
        '''
        return len(self.elements), len(self.blocks), len(self.lattice)

    def getISingEl(self, tmpName, lDebug=True):
        '''return index of SINGLE ELEMENT named tmpName from list of SINGLE
        ELEMENTs
        '''
        if lDebug:
            LOGGER.info('%s - %s' % ('getISingEl', tmpName))
        ans = [iEl for iEl in range(len(self.elements)) if (
            tmpName == self.elements[iEl]['NAME'])]
        if len(ans) == 0:
            raise ValueError('unable to find %s in list of SINGLE ELEMENTs!' %
                             (tmpName))
        elif len(ans) > 1:
            raise ValueError('%s found multiple times in list of SINGLE ELEMENTs!'
                             % (tmpName))
        else:
            ans = ans[0]
            if lDebug:
                LOGGER.info('%s has id %i in list of SINGLE ELEMENTs' %
                            (tmpName, ans))
        return ans

    def getIBlock(self, tmpName, key='NAME', lDebug=True):
        '''return index of BLOC named tmpName from list of BLOCks'''
        if lDebug:
            LOGGER.info('%s - %s - key: %s' % ('getIBlock', tmpName, key))
        if len(self.blocks) == 0:
            raise ValueError('no BLOCks in current structure!')
        if key not in self.blocks[0]:
            raise ValueError('no key in BLOCk element named %s!' % (key))
        ans = [iEl for iEl in range(len(self.blocks)) if (
            tmpName == self.blocks[iEl][key])]
        if len(ans) == 0:
            raise ValueError('unable to find %s in list of BLOCks!' %
                             (tmpName))
        elif len(ans) > 1:
            raise ValueError('%s found multiple times in list of BLOCks (key=%s)!'
                             % (tmpName, key))
        else:
            ans = ans[0]
            if lDebug:
                LOGGER.info('%s has id %i in list of BLOCKs' % (tmpName, ans))
        return ans

    def getIDriftFromBlockName(self, tmpName, lDebug=True):
        '''return index of drift in list of BLOCks and in list of SINGLE
        ELEMENTs
        '''
        iBlock = self.getIBlock(tmpName, lDebug=lDebug)
        if lDebug:
            LOGGER.info('%s is actually %s' % (self.blocks[iBlock]['NAME'],
                                               self.blocks[iBlock]['ELEM']))
        iSing = self.getISingEl(self.blocks[iBlock]['ELEM'], lDebug=lDebug)
        return iBlock, iSing

    def createDrift(self, iSing=None, L=None, lDebug=True):
        '''creates a new drift, ie the SINGLE ELEMENT and the BLOCk
        if iSing and iBlock are not None, an existing one is cloned
        '''
        if lDebug:
            LOGGER.info('%s - iSing,L,lDebug:'
                        % ('createDrift'), iSing, L, lDebug)
        # new DRIFT
        if iSing is None:
            new_SE = {}
            new_SE['TYPE'] = '0'
            for key in ['VAR1', 'VAR2', 'LENG', 'VAR4', 'VAR5', 'VAR6']:
                new_SE[key] = '0.0'
        else:
            new_SE = deepcopy(self.elements[iSing])
        # . find latest drift ID and increase it by one:
        maxID = 0
        for element in self.elements:
            if (element['NAME'].startswith('drift_')):
                currID = int(element['NAME'].split('drift_')[1])
                if (currID > maxID):
                    maxID = currID
        maxID += 1
        # . define new name
        new_SE['NAME'] = 'drift_%i' % maxID
        if new_SE['NAME'] in self.elements:
            LOGGER.error('Duplication of name in list of SINGLE ELEMENTS: %s' %
                    (new_SE['NAME']))
            LOGGER.error('Aborting....')
            raise Exception
        # . define new length, in case
        if L is not None:
            new_SE['LENG'] = "%.9e" % L
        # . append it to list of SINGLE ELEMENTs:
        self.elements.append(new_SE)
        iSing = len(self.elements)-1
        # new BLOC
        # . make a copy of existing one:
        new_BK = {}
        for key in FORT2_BLOCK_FIELDS:
            new_BK[key] = ''
        # . find latest drift ID and increase it by one:
        maxID = 0
        for block in self.blocks:
            if 'BLOC' not in block['NAME']:
                continue  # skip line right after BLOCk keyword
            currID = int(block['NAME'].split('BLOC')[1])
            if currID > maxID:
                maxID = currID
        maxID += 1
        # . define new name
        new_BK['NAME'] = 'BLOC%i' % maxID
        if new_BK['NAME'] in self.blocks:
            LOGGER.error('Duplication of name in list of BLOCks: %s' %
                    (new_BK['NAME']))
            LOGGER.error('Aborting....')
            raise Exception
        new_BK['ELEM'] = self.elements[iSing]['NAME']
        # . append it to list of SINGLE ELEMENTs:
        self.blocks.append(new_BK)
        iBlock = len(self.blocks)-1

        del(new_SE)
        del(new_BK)
        return iSing, iBlock


def read_fort2(file):
    # Reads fort.2 file in a Fort2Struct
    struct = Fort2Struct()
    line = file.readline()
    while(line != ''):
        if line.startswith('SING'):
            # Reading elements
            # Now read each element properties
            line = file.readline()
            while(not line.startswith('NEXT')):
                struct.elements.append(dict(zip(FORT2_ELEMENT_FIELDS,
                                                line.split())))
                line = file.readline()
        elif line.startswith('BLOC'):
            # Reading block definitions
            line = file.readline()
            while(not line.startswith('NEXT')):
                struct.blocks.append(dict(zip(FORT2_BLOCK_FIELDS,
                                              line.split())))
                line = file.readline()
        elif line.startswith('STRU'):
            # Reading lattice structure
            line = file.readline()
            while(not line.startswith('NEXT')):
                struct.lattice = struct.lattice + line.split()
                line = file.readline()
        else:
            while(not line.startswith('NEXT')):
                line = file.readline()
        line = file.readline()
    return struct


def write_fort2(file, struct):
    '''Writes the modified fort.2 file'''

    # Write Single Elements part
    msg = 'SINGLE ELEMENTS---------------------------------------------------------'
    file.write("%s\n" % msg)
    for element in struct.elements:
        file.write("%-16s %4s %18s %18s %18s %18s %18s %18s\n" %
                   (element['NAME'], element['TYPE'], element['VAR1'], element['VAR2'],
                    element['LENG'], element['VAR4'], element['VAR5'], element['VAR6']))
    msg = 'NEXT'
    file.write("%s\n" % msg)
    # Write Block part
    msg = 'BLOCK DEFINITIONS-------------------------------------------------------'
    file.write("%s\n" % msg)
    for block in struct.blocks:
        if block['NAME'] == '1':
            file.write("%-3s%-s\n" % (block['NAME'], block['ELEM']))
        else:
            file.write("%-18s%-18s\n" % (block['NAME'], block['ELEM']))
    msg = 'NEXT'
    file.write("%s\n" % msg)
    # Write Lattice part
    msg = 'STRUCTURE INPUT---------------------------------------------------------'
    file.write("%s\n" % msg)
    # Correct treatment of special string 'GO'
    try:
        idxgo = struct.lattice.index('GO')
    except ValueError:
        pass
    # Add GO into lattice if it was present in the initial version. try environment
    # included to avoid UnboundLocalError if GO was not there in the first place
    # (bug identified and solved by P.Hermes).
    try:
        struct.lattice[idxgo + 1] = 'GO  ' + struct.lattice[idxgo + 1]
        struct.lattice.pop(idxgo)
    except UnboundLocalError:
        pass

    for i in range(0, len(struct.lattice), 3):
        try:
            file.write("%-17s %-17s %-17s \n" % (struct.lattice[i],
                                                 struct.lattice[i + 1],
                                                 struct.lattice[i + 2]))
        except IndexError:
            try:
                file.write("%-17s %-17s \n" % (struct.lattice[i],
                                               struct.lattice[i + 1]))
            except IndexError:
                file.write("%-17s \n" % (struct.lattice[i]))
    msg = 'NEXT'
    file.write("%s\n" % msg)
    # End function


def fort2_to_twiss(struct):
    '''Transforms fort.2 in a Twiss-like structure, including the extra field
    S'''
    # Import deepcopy
    # Twiss-like structure
    sequence = []
    senames = [item['NAME'] for item in struct.elements]
    Spos = 0.e0
    for element in struct.lattice:
        if 'BLOC' not in element and element in senames:
            # If not BLOC, add single element to sequence
            for single in struct.elements:
                if element == single['NAME']:
                    length = float(single['LENG'])
                    if (length > 0.e0):
                        if (single['TYPE'] != '12'):
                            # skip cavities
                            Spos += length
                    dict = deepcopy(single)
                    dict['S'] = Spos
                    dict['BLOC'] = False
                    dict['SPECIAL'] = False
                    sequence.append(dict)
        elif element not in senames and 'BLOC' not in element:
            msg = 'WARNING: Element in lattice: '+str(element)
            msg = msg+", with no corresponding single element.\n"
            LOGGER.info(msg)
            dict = {'VAR5': '0.000000000e+00', 'VAR4': '0.000000000e+00',
                    'VAR6': '0.000000000e+00', 'LENG': '0.000000000e+00',
                    'NAME': element, 'VAR2': '0.000000000e+00', 'S': Spos,
                    'BLOC': False, 'SPECIAL': True,
                    'VAR1': '0.000000000e+00', 'TYPE': '0'}
            sequence.append(dict)
        else:
            # If BLOC, jump from lattice to bloc and from bloc to single
            for block in struct.blocks:
                if element == block['NAME']:
                    for single in struct.elements:
                        if block['ELEM'] == single['NAME']:
                            length = float(single['LENG'])
                            if length > 0.e0:
                                Spos += length
                            dict = deepcopy(single)
                            dict['S'] = Spos
                            dict['BLOC'] = True
                            dict['SPECIAL'] = False
                            sequence.append(dict)
    # End of function
    return sequence


def twiss_to_fort2(sequence):
    '''Transforms a twiss-like structure into fort.2. The twiss structure
    should contain the same fields as the output in fort2_to_twiss
    '''
    struct = Fort2Struct()
    seen_elem = []
    seen_bloc = []
    b_idx = 1
    struct.blocks.append({'NAME': '1', 'ELEM': '1'})
    for item in sequence:
        # Filling up the SINGLE ELEMENT definitions
        if item['NAME'] not in seen_elem and not item['SPECIAL']:
            struct.elements.append(item)
            seen_elem.append(item['NAME'])
        # Filling up the BLOCK definitions
        if item['BLOC']:
            name = 'BLOC' + str(b_idx)
            if item['NAME'] not in seen_bloc:
                struct.blocks.append({'NAME': name, 'ELEM': item['NAME']})
                seen_bloc.append(item['NAME'])
                b_idx += 1
        # Filling up the LATTICE structure
        if item['BLOC']:
            for block in struct.blocks:
                if block['ELEM'] == item['NAME']:
                    struct.lattice.append(block['NAME'])
        else:
            struct.lattice.append(item['NAME'])
    return struct
