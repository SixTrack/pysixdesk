import os
import sys

class LHC(object):

    def __init__(self, runtype):
        self.runtype = runtype
        self.params = {}
        self.default()

    def default(self):
        inj = {}
        inj['RF_voltage'] = 8.0 #[MV]
        inj['sigz'] = 0.11 #[mm] r.m.s. bunch length
        inj['sige'] = 4.5e-04 #[] r.m.s. energy spread

        col = {}
        col['RF_voltage'] = 16.0 #[MV]
        col['sigz'] = 0.77e-1 #[mm]
        col['sige'] = 1.1e-04 #[]

        self.params['inj'] = inj
        self.params['col'] = col

    def __getitem__(self, var):
        return self.params[self.runtype][var]

class HLLHC(object):

    def __init__(self, runtype):
        self.runtype = runtype
        self.params = {}
        self.default()

    def default(self):
        inj = {}
        inj['RF_voltage'] = 8.0 #[MV]
        inj['sigz'] = 0.11 #[mm]
        inj['sige'] = 4.5e-04 #[]

        col = {}
        col['RF_voltage'] = 16.0 #[MV]
        col['sigz'] = 0.77e-1 #[mm]
        col['sige'] = 1.1e-04 #[]

        self.params['inj'] = inj
        self.params['col'] = col

    def __getitem__(self, var):
        return self.params[self.runtype][var]
