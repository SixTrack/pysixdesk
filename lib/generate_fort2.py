#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Author: Pablo Garcia Ortega <pgarciao@cern.ch>
# Last changes: 42/06/2014

# Program created for including aperture markers in Sixtrack fort.2 file
# from a TWISS file, for the Sixtrack-FLUKA coupling
import os
import sys
from copy import deepcopy

from twiss_tools import *
from fort2_tools import *


# New keys for apertures with offsets
VALUES_off = ['APER_1', 'APER_2', 'APER_3', 'APER_4', 'XOFF', 'YOFF']


def run(fc2, aperture, survery=None, ldebug=False, lold=False):
    # Open fc.2 file
    rfile = open(fc2, 'r')
    # Open aperture file
    tfile = open(file2, 'r')
    if survery is None:
        sfile = False
    else:
        sfile = open(survery, 'r')

    ofile_name = 'fort.2'
    ofile = open(ofile_name, 'w')
    lfile = open('fort3.limi', 'w')
    # Parse structure of Fort.2 file
    print('\nReading fort.2 file: %s ...' % (fc2))
    F2struct = read_fort2(rfile)
    # get dimension of constitutive arrays (ie SINGLE ELEMENTs, BLOCs, LATTICE ELEMENTs)
    NSEorig, NBLorig, NLTorig = F2struct.echoDimensions()
    # Parse structure of Aperture Twiss file
    print('\nReading aperture file: %s ...' % (aperture))
    TWstruct = read_twiss(tfile)
    print('...read %i elements in total (including DRIFTs);' %
            (len(TWstruct.elements)))
    if ldebug:
        print 'Dumping aperture makers in TWstruct0.dat ...'
        file0 = open("TWstruct0.dat", 'w')
        for item in TWstruct.elements:
            file0.write("%-16s %12s %12s %12s %12s %12s %12s\n" %
                        (item['NAME'], item['S'], item['L'],
                         item['APER_1'], item['APER_2'], item['APER_3'], item['APER_4'],))
        file0.close()
    # Clean and compress apertures
    print('\nCleaning apertures: removing redundances and zero apertures...')
    TWstruct = clean_apertures(TWstruct)
    print('   ...down to %i elements;' % (len(TWstruct.elements)))
    # Add offsets to TWstruct
    for item in TWstruct.elements:
        item['XOFF'] = 0.0
        item['YOFF'] = 0.0
    if ldebug:
        print 'Dumping aperture makers in TWstruct1.dat ...'
        file1 = open("TWstruct1.dat", 'w')
        for item in TWstruct.elements:
            file1.write("%-16s %12s %12s %12s %12s %12s %12s %12s %12s\n" %
                        (item['NAME'], item['S'], item['L'],
                         item['APER_1'], item['APER_2'], item['APER_3'], item['APER_4'],
                         str(item['XOFF']), str(item['YOFF'])))
        file1.close()
    # here, TWstruct contains only the necessary markers as from aperture model
    # Parse and clean survey file, if required
    if sfile:
        print('\nReading survey file: %s ...' % (survery))
        SUstruct, SUregions = read_survey(sfile)
        print('...for a total of %s active markers;' % (len(SUstruct)))
        if len(SUstruct) == 0:
            sfile = False
    if ldebug and sfile:
        print('Dumping info from survey file survey0.dat ...')
        file0 = open("survey0.dat", 'w')
        for item in SUstruct:
            file0.write("%12s %12s\n" % (str(item['s[m]']), str(item['Xs[m]'])))
        for item in SUregions:
            file0.write("%12s %12s\n" % (str(item[0]), str(item[1])))
        file0.close()
    # Merge TWstruct with SUstruct, interpolating the offsets in a way
    # the linear interpolation of offsets works fine
    if sfile:
        TWstruct = merge_survey(TWstruct, SUstruct, SUregions)
        if ldebug:
            print('Dumping aperture makers in TWstruct2.dat ...')
            file1 = open("TWstruct2.dat", 'w')
            for item in TWstruct.elements:
                file1.write("%-16s %12s %12s %12s %12s %12s %12s %12s %12s\n" %
                            (item['NAME'], item['S'], item['L'],
                             item['APER_1'], item['APER_2'], item['APER_3'], item['APER_4'],
                             str(item['XOFF']), str(item['YOFF'])))
            file1.close()
    # here, TWstruct contains markers from survey and markers from aperture model,
    #       in increasing order of S
    # Transform Fort.2 structure to Twiss structure
    F2sequence = fort2_to_twiss(F2struct)
    F2names = [item['NAME'].upper() for item in F2sequence]
    if (ldebug):
        print('Dumping oiginal sequence contained in fort.2 in fort.2_1.log ...')
        file1 = open("fort.2_1.log", 'w')
        for item in F2sequence:
            file1.write("%-16s %12.4f\n" % (item['NAME'], item['S']))
        file1.close()
    # Assign apertures to fort.2 lenses, interpolated as done in BeamLossPattern
    print('\n\nAssigning apertures to fort.2 lenses interpolated as done in
            BeamLossPattern ...')
    Lenselist = assign_apertures(F2sequence, TWstruct)
    # here, TWstruct merges markers from survey onto list of necessary markers from aperture model
    # Add new apertures to list of apertures
    TWstruct.elements += Lenselist
    # here, TWstruct contains:
    # - markers from aperture and survey;
    # - lenses in fort.2 (type!=0 -> no drifts, including markers!);
    # Write out apertures
    if ldebug:
        print('Dumping aperture makers in aper0.dat ...')
        file0 = open("aper0.dat", 'w')
        for item in TWstruct.elements:
            file0.write("%-16s %12s %12s %12s %12s %12s %12s %12s %12s\n" %
                        (item['NAME'], item['S'], item['L'],
                         item['APER_1'], item['APER_2'], item['APER_3'], item['APER_4'],
                         str(item['XOFF']), str(item['YOFF'])))
        file0.close()
    # Rename apertures
    print('\nRenaming apertures to reduce number of new single elements...')
    TWstruct = rename_apertures(TWstruct)
    # Write out apertures
    if ldebug:
        print('Dumping aperture makers in aper1.dat ...')
        file1 = open("aper1.dat", 'w')
        for item in TWstruct.elements:
            file1.write("%-16s %12s %12s %12s %12s %12s %12s %12s %12s\n" %
                        (item['NAME'], item['S'], item['L'],
                         item['APER_1'], item['APER_2'], item['APER_3'], item['APER_4'],
                         str(item['XOFF']), str(item['YOFF'])))
        file1.close()
    # Index for new drifts
    Dftidx = 1
    # Define list for apertures in the format of the LIMI block
    Aperlimi = []
    # Add non-zero apertures to fort.2 as markers
    print('\nAdd non-zero apertures to fort.2 as markers...')
    idx = 0
    for aperture in TWstruct.elements:
        idx += 1
        ap_name = aperture['NAME']
        # If aperture element is zero-length we add a marker at that position
        if float(aperture['L']) == 0.e0:
            # But only if the element is not already in the fort.2
            # We use the aperture defined in the twiss file
            # N.B: If collimator markers have already been moved,
            # the S value can be shifted half collimator length! Not big deal
            if ap_name not in F2names:
                s_pos = float(aperture['S'])
                F2sequence = add_aperture(F2sequence, ap_name.lower(),
                                          s_pos, Dftidx)
                Dftidx += 1
            # Add aperture to LIMI block
            Aperlimi.append(aperture_type(ap_name.lower(), aperture))
            # If aperture element is thick, divide it in two zero-length
            # elements, at the beggining (S-L) and at the end (S)
            # with the same aperture! N.B: Not it does not matter
            # if the element was in the fort.2 or not
        else:
            # Start marker
            s_pos = float(aperture['S'])-float(aperture['L'])
            s_name = 'sm_'+ap_name.lower()
            F2sequence = add_aperture(F2sequence, s_name, s_pos, Dftidx)
            Aperlimi.append(aperture_type(s_name, aperture))
            Dftidx += 1
            # End marker
            s_pos = float(aperture['S'])
            s_name = 'em_'+ap_name.lower()
            F2sequence = add_aperture(F2sequence, s_name, s_pos, Dftidx)
            Aperlimi.append(aperture_type(s_name, aperture))
            Dftidx += 1
    # check name lengths and correct accordingly
    print('\n\nCheking name lengths...')
    F2sequence, Aperlimi = checkNameLengths(F2sequence, Aperlimi)
    # Transform twiss-like sequence back to fort.2
    print('\n\nCreating %s file ... ' % (ofile_name))
    # convert sequence from twiss to fort.2
    newF2struct = twiss_to_fort2(F2sequence)
    # get dimension of constitutive arrays (ie SINGLE ELEMENTs, BLOCs, LATTICE ELEMENTs)
    NSEnew, NBLnew, NLTnew = newF2struct.echoDimensions()
    # Dump fort.2 struct into file
    write_fort2(ofile, newF2struct)
    # Dump apertures in limi format
    lfile.write("/ %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n" %
                ('NAME', 'APERTYPE', 'APER_1', 'APER_2', 'APER_3',
                 'APER_4', 'ANGLE', 'XOFF', 'YOFF'))
    # From meters in MADX to mm in SixTrack
    MtoMM = 1000.0
    seenaper = []
    for aperture in Aperlimi:
        if aperture['NAME'] not in seenaper:
            if lold:
                lfile.write("%-16s %-3s %10.4e %10.4e %10.4e %10.4e %10.4e %10.4e %10.4e\n" %
                            (aperture['NAME'], aperture['APERTYPE'],
                             MtoMM*aperture['APER_1'], MtoMM*aperture['APER_2'],
                             MtoMM*aperture['APER_3'], MtoMM*aperture['APER_4'],
                             aperture['ANGLE'], MtoMM*aperture['XOFF'],
                             MtoMM*aperture['YOFF']))
            else:
                lfile.write("%-16s %-3s %10.4e %10.4e %10.4e %10.4e %10.4e %10.4e %10.4e\n" %
                            (aperture['NAME'], aperture['APERTYPE'],
                             MtoMM*aperture['APER_1'], MtoMM*aperture['APER_2'],
                             MtoMM*aperture['APER_3'], MtoMM*aperture['APER_4'],
                             -MtoMM*aperture['XOFF'], -MtoMM*aperture['YOFF'],
                             aperture['ANGLE'], ))
            seenaper.append(aperture['NAME'])
    # check some numbers for consistency
    print('')
    print(' dimensions of arrays in fort.2:')
    print(' %16s | %10s | %10s | %10s' % ('', 'original', 'new', 'variation'))
    print(' %16s | %10i | %10i | %10s' % ('SINGLE ELEMENTs', NSEorig, NSEnew,
        NSEnew-NSEorig))
    print(' %16s | %10i | %10i | %10s' % ('BLOCs', NBLorig, NBLnew, NBLnew-NBLorig)
    print(' %16s | %10i | %10i | %10s' % ('LATTICE ELEMENTs', NLTorig, NLTnew,
        NLTnew-NLTorig))
    print('')
    print('...%i entries in LIMI block;' % (len(seenaper)))
    print('...delta BLOCs + entries in LIMI block = %i;' %
        (len(seenaper)+(NBLnew-NBLorig)))
    tfile.close()
    rfile.close()
    if sfile:
        sfile.close()
    ofile.close()
    lfile.close()
    # Some checks
    file1 = open("new_optics.tfs", 'w')
    Index = 0
    for item in F2sequence:
        Index += 1
        file1.write("%6i %-16s %12.5f\n" % (Index, item['NAME'], item['S']))
    file1.close()
    print('...done.')


def error_message(tmp_string, labort):
    '''function for printing on screen an error message:
    labort = True:  python job abort is called;
    labort = False: the message is printed only, the job will go on
    '''
    tmp_string = tmp_string.strip()
    # in case of aborting, add a clear abort message on screen
    if labort:
        if len(tmp_string) > 0:
            tmp_string = tmp_string + "\n" + "Aborting" + "... "
        else:
            tmp_string = "Aborting" + "... "
    # strip strings:
    tmp_strings = tmp_string.split("\n")
    # print message:
    print("!!")
    for single_string in tmp_strings:
        print(" !! " + single_string.strip())
    print(" !! ")
    print ""
    if labort:
        exit(1)


def read_survey(file1, dS=0.001):
    '''Read Survey file
    dS=shift in s-coordinate [m] for inserting 0-offset elements before/after
    survey segment
    '''
    # Define survey list
    struct = []
    # Temp flag
    iszero = True
    # List of tuples with min/max values of nonzero offset regions
    regions = []
    # Read file
    for line in file1.readlines():
        line = line.strip()
        if line.startswith('%'):
            # Header with element fields
            d = ']'
            element_fields = [
                e+d for e in line[1:].strip().replace(' ', '').split(d) if e != ""]
            # If survey column not found, stop reading
            if 'Xs[m]' not in element_fields:
                print('Error reading Survey file! Xs[m] column not found!')
                print('The offset info will not be considered')
                break
            else:
                continue
        # read element properties
        element_data = line.split()
        item = dict(zip(element_fields, element_data))

        # Only store the non-zero values and transitions
        if float(item['Xs[m]']) != 0.0:
            # Add last zero value
            if iszero:
                min = float(item['s[m]'])-dS
                struct.append(zeroit)
                struct[-1]['s[m]'] = str(min)
                iszero = False
            struct.append(item)
        else:
            zeroit = item
            # Add last nonzero value
            if not iszero:
                max = float(struct[-1]['s[m]'])+dS
                regions.append((min, max))
                struct.append(zeroit)
                struct[-1]['s[m]'] = str(max)
                iszero = True
    return struct, regions


def merge_survey(TWstruct, SUstruct, SUregions):
    '''Merge survey data with the aperture model:
    Assign an interpolate aperture to the survey points from the aperture model
    '''
    # Zero aperture
    newaper = {'NAME': '', 'PARENT': 'MARKER', 'L': '0.000000', 'APER_1': 0,
               'S': 0, 'APER_2': 0, 'APER_4': 0, 'KEYWORD': 'MARKER', 'APER_3': 0,
               'XOFF': 0, 'YOFF': 0}
    # New list with survey apertures added,
    # and the apertures between survey regions removed
    NewTWstruct = TwissStruct()
    # index for new aperture names
    SUidx = 1
    for j in range(1, len(TWstruct.elements)):
        # Store position and length of previous and next apertures
        TWpos = float(TWstruct.elements[j]['S'])
        TWpre = float(TWstruct.elements[j-1]['S'])
        TWlng = float(TWstruct.elements[j]['L'])
        # Check if we have survey points between elements
        Survey = False
        for k in range(len(SUregions)):
            if (TWpre <= SUregions[k][0] and TWpos >= SUregions[k][0]) or \
               (TWpre <= SUregions[k][1] and TWpos >= SUregions[k][1]) or \
               (TWpre >= SUregions[k][0] and TWpos <= SUregions[k][1]):
                SUmin = SUregions[k][0]
                SUmax = SUregions[k][1]
                Survey = True
                break
        # If the present aperture inside a survey region, add to NewTWstruct
        TWout = True
        for k in range(len(SUregions)):
            if TWpre >= SUregions[k][0] and TWpre <= SUregions[k][1]:
                TWout = False
                break
        # If previous Twiss element outside survey region, add to NewTWstruct
        if TWout:
            NewTWstruct.elements.append(TWstruct.elements[j-1])
        # If we do have a survey point between elements, create survey apertures by interpolation
        if Survey:
            for item in SUstruct:
                SUpos = float(item['s[m]'])
                # Check we are in the region of interest,
                # if not, jump to next item
                if SUpos < SUmin or SUpos > SUmax:
                    continue
                # Set new aperture
                SUaper = deepcopy(newaper)
                SUaper['NAME'] = 'surv.'+str(SUidx)
                SUaper['S'] = str(item['s[m]'])
                SUaper['XOFF'] = float(item['Xs[m]'])
                SUaper['YOFF'] = 0.0000000

                # Take care if we have a thick element
                if SUpos < (TWpos-TWlng) and SUpos >= TWpre:
                    # Interpolate, careful with the length of the next element
                    # the interpolation must go from the latest point of the previous aperture to the
                    # earliest point of the next aperture!!!
                    param = (SUpos-TWpre)/(TWpos-TWlng-TWpre)
                    for kw in TWstruct.APER_VALUES:
                        apre = float(TWstruct.elements[j-1][kw])
                        apos = float(TWstruct.elements[j][kw])
                        SUaper[kw] = str(apre + (apos-apre)*param)
                    # Add to NewTWstruct
                    NewTWstruct.elements.append(SUaper)
                    # Counter update
                    SUidx += 1
                # If the aperture is inside a thick element
                # I take the same aperture as the element
                elif SUpos <= TWpos and SUpos >= (TWpos-TWlng):
                    for kw in TWstruct.APER_VALUES:
                        SUaper[kw] = str(TWstruct.elements[j][kw])
                    # Add to NewTWstruct
                    NewTWstruct.elements.append(SUaper)
                    # Counter update
                    SUidx += 1
            del SUaper
    # Add last TWstruct element (as it is not added in the previous loop)
    NewTWstruct.elements.append(TWstruct.elements[-1])
    return NewTWstruct


def add_aperture(sequence, name, position, index, precision=1.e-04):
    '''Add aperture marker to fort.2 file sequence'''
    # Define aperture marker
    zeroel = {'VAR5': '0.000000000e+00', 'VAR4': '0.000000000e+00',
              'VAR6': '0.000000000e+00', 'LENG': '0.000000000e+00',
              'NAME': '', 'VAR2': '0.000000000e+00', 'S': 0.0,
              'BLOC': False, 'SPECIAL': False,
              'VAR1': '0.000000000e+00', 'TYPE': '0'}
    temp = deepcopy(zeroel)
    temp['NAME'] = name
    temp['S'] = position
    names = [item['NAME'].lower() for item in sequence]

    for j in range(len(sequence)):
        item = sequence[j]
        if abs(float(item['S'])-position) <= precision:
            # Element in the same position
            # Check if next element in sequence is also in the same position
            # jump to next iteration
            if (j < len(sequence)-1):
                if abs(float(sequence[j+1]['S'])-position) <= precision:
                    continue
            # We add the new aperture after the item in the same position
            # To keep the original order of apertures
            sequence.insert(j+1, temp)
            # print position, sequence[j-1]['NAME'],sequence[j]['NAME'],\
            #    sequence[j+1]['NAME'],'...',item['NAME'],temp['NAME']
            # No further modification
            break
        if float(item['S']) > position:
            # Element just above the marker position.
            # It is a drift, we divide it in two and add the marker
            d_length = float(item['LENG'])
            item['LENG'] = "%15.9e" % (float(item['S'])-position)
            # Reset the drift name and scan if there are previous
            # drifts with the same name. If so, use the first name
            First = True
            for i in range(j):
                if item['BLOC'] and item['LENG'] == sequence[i]['LENG']:
                    if First:
                        firstname = sequence[i]['NAME']
                        First = False
                    item['NAME'] = firstname
                    sequence[i]['NAME'] = firstname
            if First:
                item['NAME'] = 'dft_a' + str(index)
            if float(item['S'])-position < 0.e0:
                msg = 'Negative drift: Error'
                error_message(msg, True)
            # Aperture marker
            sequence.insert(j, temp)
            # Add drift before marker
            remainLength = d_length-float(item['LENG'])
            if abs(remainLength) > precision:
                # we need to insert a drift:
                drift = deepcopy(zeroel)
                drift['LENG'] = "%15.9e" % (remainLength)
                drift['S'] = position
                drift['BLOC'] = True
                # Reset the drift name and scan if there are previous
                # drifts with the same name. If so, use the first name
                First = True
                for i in xrange(j):
                    if drift['LENG'] == sequence[i]['LENG']:
                        if First:
                            firstname = sequence[i]['NAME']
                            First = False
                        drift['NAME'] = firstname
                        sequence[i]['NAME'] = firstname
                if First:
                    drift['NAME'] = 'dft_b' + str(index)
                sequence.insert(j, drift)
                if d_length-float(item['LENG']) < 0.e0:
                    msg = 'Negative drift: Error'
                    error_message(msg, True)
                del drift
            break
    del temp
    return sequence


def aperture_type(name, aperture):
    '''Transform BeamLossPattern aperture type into LIMI block aperture type'''
    # New definition of aperture for LIMI
    newaperture = {'NAME': name, 'APERTYPE': '', 'APER_1': 0, 'APER_2': 0,
                   'APER_3': 0, 'APER_4': 0, 'ANGLE': 0, 'XOFF': 0, 'YOFF': 0}
    # Aperture list
    aper = [float(aperture['APER_1']), float(aperture['APER_2']),
            float(aperture['APER_3']), float(aperture['APER_4']),
            float(aperture['XOFF']), float(aperture['YOFF'])]
    # Check apertures, as in BeamLossPattern_2005-04-26.cpp
    if ((aper[0] != 0.e0 and aper[1] == 0) or
            (aper[0] == 0.e0 and aper[1] != 0.e0)):
        msg = "ERROR: Invalid aperture definition for "+name
        msg = msg + " ... apertures: " + str(aper)
        error_message(msg, True)
    # Now we redefine the apertures as read by SixTrack
    # There is no ELLIPSE definition, probably absorbed into
    # the RECTELLIPSE aperture
    # RECTANGLE APERTURE
    if aper[0] != 0.e0 and aper[2] == 0.e0:
        newaperture['APERTYPE'] = 'RE'
        newaperture['APER_1'] = aper[0]
        newaperture['APER_2'] = aper[1]
        newaperture['APER_3'] = 0.e0
        newaperture['APER_4'] = 0.e0
        # Angle (in DEG)
        newaperture['ANGLE'] = aper[3]
    # RECTELLIPSE APERTURE
    if aper[0] != 0.e0 and aper[2] > 0.e0:
        # ELLIPSE APERTURE
        if aper[0] >= aper[2] and aper[1] >= aper[3]:
            newaperture['APERTYPE'] = 'EL'
            newaperture['APER_1'] = aper[2]
            newaperture['APER_2'] = aper[3]
            newaperture['APER_3'] = 0.e0
            newaperture['APER_4'] = 0.e0
        # RECTELLIPSE APERTURE
        else:
            newaperture['APERTYPE'] = 'RL'
            newaperture['APER_1'] = aper[0]
            newaperture['APER_2'] = aper[1]
            newaperture['APER_3'] = aper[2]
            newaperture['APER_4'] = aper[3]
    # RACETRACK APERTURE
    if aper[0] == 0.e0:
        newaperture['APERTYPE'] = 'RT'
        # The definition of Racetrack in BeamLossPattern is special,
        # with radius=height/2 always
        newaperture['APER_1'] = aper[2]
        newaperture['APER_2'] = aper[3]
        newaperture['APER_3'] = aper[3]
        newaperture['APER_4'] = 0.e0
    if newaperture['APERTYPE'] == '':
        msg = 'ERROR: Unknown aperture type for '+name
        msg = msg + " ... apertures: " + str(aper)
        error_message(msg, True)
    # Copy offsets
    newaperture['XOFF'] = aper[4]
    newaperture['YOFF'] = aper[5]
    return newaperture

def clean_apertures(struct):
    '''Clean apertures (take only non-zero apertures and remove redundant ones)
    I remove apertures if they only have drifts in between and
    the same aperture values, so I leave only the first and the last aperture,
    the middle one are redundant. Two are enough
    for a backtracking, if they only have drifts between them.
    Here we don't need to include the offsets
    '''
    newstruct = TwissStruct()
    first_pos = 0
    old_pos = 0
    # Remove redundant apertures
    for j in range(len(struct.elements)):
        aperture = struct.elements[j]
        # Normal aperture, no lense
        if not empty_aperture(aperture):
            # Old aperture?
            if compare_aperture(aperture, struct.elements[old_pos]):
                # Store position as old_pos and go on
                old_pos = j
            # New aperture?
            else:
                # Store old aperture, if not first
                if old_pos != first_pos:
                    newstruct.elements.append(struct.elements[old_pos])
                # Set aperture as First, set position as first_position
                # and old_position, store aperture
                newstruct.elements.append(aperture)
                first_pos = j
                old_pos = j
    return newstruct


def rename_apertures(struct):
    '''Rename apertures to reduce number of new single elements'''
    newstruct = deepcopy(struct)
    seenaper = []
    for j in range(len(newstruct.elements)):
        aperture = newstruct.elements[j]
        name = aperture['NAME'].split('.')
        # See if aperture name already changed,
        # or it is a survey aperture (they never coincide and offsets are not
        # checked!)
        if 'APER' in name or 'surv' in name:
            continue
        # If not, change it and scan newstruct for equal apertures
        rootname = name[0]+'.APER'
        if len(rootname) > 12:
            rootname = 'sel_APER'
        seenaper.append(rootname)
        index = len([x for x in seenaper if rootname == x])
        newname = rootname+'.'+str(index)
        for i in range(j, len(newstruct.elements)):
            if compare_aperture(aperture, newstruct.elements[i], VALUES_off):
                newstruct.elements[i]['NAME'] = newname
        del newname, rootname
    return newstruct


def checkNameLengths(tmpSequence, tmpAperLimi, maxLen=16):
    lErr = False
    impossibleNames = []
    changeNames = []
    for jj in range(len(tmpSequence)):
        if (len(tmpSequence[jj]['NAME']) > maxLen):
            origName = tmpSequence[jj]['NAME']
            # cut down name length
            # - try changing 'aper' in 'ap'
            tmpSequence[jj]['NAME'] = tmpSequence[jj]['NAME'].replace(
                'aper', 'ap')
            # - try removing '_' and '.' (one at time):
            for tmpChar in ['_', '.']:
                while(len(tmpSequence[jj]['NAME']) > maxLen and tmpSequence[jj]['NAME'].count(tmpChar) > 0):
                    tmpSequence[jj]['NAME'] = tmpSequence[jj]['NAME'].replace(
                        tmpChar, '', 1)
            if len(tmpSequence[jj]['NAME']) > maxLen:
                if origName not in impossibleNames:
                    impossibleNames.append(origName)
                lErr = True
            else:
                # update name also in tmpAperLimi array:
                for kk in range(len(tmpAperLimi)):
                    if tmpAperLimi[kk]['NAME'] == origName:
                        tmpAperLimi[kk]['NAME'] = tmpSequence[jj]['NAME']
                # notify user about change:
                if (origName not in changeNames):
                    # ...but only once!
                    changeNames.append(origName)
                    print(' ...%s changed into %s !' % (origName,
                        tmpSequence[jj]['NAME']))
    if lErr:
        msg = 'unable to shorten the following names:\n'
        for impossibleName in impossibleNames:
            msg = msg + '%s\n' % (impossibleName)
        msg = msg + "they are longer than %i chars" % (maxLen)
        error_message(msg, True)
    return tmpSequence, tmpAperLimi


def assign_apertures(F2sequence, TWstruct):
    '''Assign apertures to fort.2 lenses, interpolated as done in
    BeamLossPattern'''
    newaper = {'NAME': '', 'PARENT': 'MARKER', 'L': '0.000000', 'APER_1': 0,
               'S': 0, 'APER_2': 0, 'APER_4': 0, 'KEYWORD': 'MARKER', 'APER_3': 0,
               'XOFF': 0.0, 'YOFF': 0.0}
    Lenselist = []
    for item in F2sequence:
        # Is it lense?
        if item['TYPE'] != '0':
            # Set new aperture
            F2aper = deepcopy(newaper)
            F2aper['NAME'] = item['NAME'].upper()
            F2aper['S'] = str(item['S'])
            F2pos = float(item['S'])
            # Iterate over apertures
            for j in range(1, len(TWstruct.elements)):
                TWpos = float(TWstruct.elements[j]['S'])
                TWpre = float(TWstruct.elements[j-1]['S'])
                TWlng = float(TWstruct.elements[j]['L'])
                # Search surrounding apertures
                if F2pos < (TWpos-TWlng) and F2pos > TWpre:
                    # Interpolate
                    param = (F2pos-TWpre)/(TWpos-TWpre)
                    for kw in VALUES_off:
                        apre = float(TWstruct.elements[j-1][kw])
                        apos = float(TWstruct.elements[j][kw])
                        F2aper[kw] = str(apre + (apos-apre)*param)
                    # Add to Lenselist
                    Lenselist.append(F2aper)
                    break
                # If the lense is inside a thick element
                # I take the same aperture as the element
                elif F2pos < TWpos and F2pos > (TWpos-TWlng):
                    for kw in VALUES_off:
                        F2aper[kw] = str(TWstruct.elements[j][kw])
                    # Add to Lenselist
                    Lenselist.append(F2aper)
                    break
            del F2aper
    return Lenselist

'''
    if len(args) >= 5:
        if (args[4] == '--printOldFormat'):
            print('\nrequested printing of aperture markers for SixTrack releases
                    >= 5.1.0')
            lold = True
        else:
            msg = 'Could not understand last terminal-line option. Did you mean --printOldFormat ?'
            error_message(msg, True)
            lold = None
    else:
        print('\nrequested printing of aperture markers for SixTrack releases <
                5.1.0')
        lold = False
'''
