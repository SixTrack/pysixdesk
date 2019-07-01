import os
import sys

class Machine(object):

    def __init__(self, machine, runtype):
        self.runtype = runtype
        self.params = {}
        if machine.lower() == 'lhc':
            LHC.parameters(self.params)
        elif machine.lower() == 'hllhc':
            HLLHC.parameters(self.params)

    def __getitem__(self, var):
        return self.params[self.runtype][var]

class LHC(object):

    @staticmethod
    def parameters(params):
        inj = {}
        inj['RF_voltage'] = 8.0 #[MV]
        inj['sigz'] = 0.11 #[mm]
        inj['sige'] = 4.5e-04 #[]

        col = {}
        col['RF_voltage'] = 16.0 #[MV]
        col['sigz'] = 0.77e-1 #[mm]
        col['sige'] = 1.1e-04 #[]

        params['inj'] = inj
        params['col'] = col

class HLLHC(object):

    @staticmethod
    def parameters(params):
        inj = {}
        inj['RF_voltage'] = 8.0 #[MV]
        inj['sigz'] = 0.11 #[mm]
        inj['sige'] = 4.5e-04 #[]

        col = {}
        col['RF_voltage'] = 16.0 #[MV]
        col['sigz'] = 0.77e-1 #[mm]
        col['sige'] = 1.1e-04 #[]

        params['inj'] = inj
        params['col'] = col
