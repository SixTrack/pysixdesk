class ParentMachine(object):
    '''Parent class representing an empty machine'''

    def __init__(self):
        self.params = {
            'inj': {
                'rfvol': None,
                'sigz': None,
                'sige': None
            },
            'col': {
                'rfvol': None,
                'sigz': None,
                'sige': None
            }
        }
        return

    def parameters(self, runtype):
        return self.params[runtype]


class LHC(ParentMachine):
    def __init__(self):
        ParentMachine.__init__(self)

        self.params['inj']['rfvol'] = 8.0  # [MV]
        self.params['inj']['sigz'] = 0.11  # [mm]
        self.params['inj']['sige'] = 4.5e-04  # []

        self.params['col']['rfvol'] = 16.0  # [MV]
        self.params['col']['sigz'] = 0.77e-1  # [mm]
        self.params['col']['sige'] = 1.1e-04  # []


class HLLHC(ParentMachine):
    def __init__(self):
        ParentMachine.__init__(self)

        self.params['inj']['rfvol'] = 8.0  # [MV]
        self.params['inj']['sigz'] = 0.11  # [mm]
        self.params['inj']['sige'] = 4.5e-04  # []

        self.params['col']['rfvol'] = 16.0  # [MV]
        self.params['col']['sigz'] = 0.77e-1  # [mm]
        self.params['col']['sige'] = 1.1e-04  # []


class MachineConfig(object):
    def __init__(self, machine, runtype):
        if machine.lower() == 'lhc':
            return LHC().parameters(runtype)
        elif machine.lower() == 'hllhc':
            return HLLHC().parameters(runtype)
        else:
            print("Machine %s not available" % machine)
