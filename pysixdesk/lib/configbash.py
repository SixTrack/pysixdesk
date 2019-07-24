import os
import copy
import math

from .study import Study


class BashStudy(Study):

    def __init__(self, name='study', location='.'):
        super(BashStudy, self).__init__(name, location)

    def initialize(self, mfile, *files):
        '''
        Set up a study from the initial files of old version sixdesk,
        e.g. sixdeskenv,sixenv
        '''
        vals = {}
        mvals = self.parse_bash_script(mfile)
        if os.path.isfile('cob_env.sh'):
            os.remove('cob_env.sh')
        f_out = open('cob_env.sh', 'w')
        for a in files:
            if os.path.isfile(a):
                f_in = open(a, 'r')
                f_lines = f_in.readlines()
                f_in.close()
                f_out.writelines(f_lines)
            else:
                print("The input file %s doesn't exist!" % a)
        f_out.close()
        val1 = self.parse_bash_script('cob_env.sh')
        os.remove('cob_env.sh')
        vals.update(val1)

        seed_i = vals['istamad']
        seed_e = vals['iendmad']
        scan_vars = mvals['scan_variables'].split()
        scan_hols = mvals['scan_placeholders'].replace('%', '').split()
        scan_vals = []
        for a in scan_vars:
            val = mvals['scan_vals_' + a]
            val = val.split()
            val = [num(i) for i in val if not math.isnan(num(i))]
            if len(val) == 1:
                val = [k + 1 for k in range(int(val[0]))]
            scan_vals.append(val)
        s_i = num(seed_i)
        s_e = num(seed_e)
        seeds = [j + s_i for j in range(int(s_e - s_i + 1))]
        scan_hols.append('SEEDRAN')
        scan_vals.append(seeds)
        for i in range(len(scan_vals)):
            self.madx_params[scan_hols[i]] = scan_vals[i]

        self.madx_input["mask_file"] = 'hl10.mask'
        self.oneturn_sixtrack_input['temp'] = [
            'fort.3.mother1', 'fort.3.mother2']
        self.oneturn_sixtrack_output = [
            'mychrom', 'betavalues', 'sixdesktunes']
        self.sixtrack_input['temp'] = ['fort.3.mother1', 'fort.3.mother2']
        self.sixtrack_input['input'] = copy.deepcopy(self.madx_output)
        self.update_tables()

    def parse_bash_script(self, mfile):
        '''parse the bash input file for the old version sixdesk'''
        mf_in = open(mfile, 'r')
        mf_lines = mf_in.readlines()
        mf_in.close()
        if os.path.isfile('mexe.sh'):
            os.remove('mexe.sh')
        mf_exe = open('mexe.sh', 'w')
        mf_exe.write('#!/bin/bash\n')
        coms = []
        params = []
        for line in mf_lines:
            if '#' not in line:
                mf_exe.write(line)
                if 'export' in line:
                    m_ar = line.split()
                    if len(m_ar) > 1:
                        m_ar1 = m_ar[1].split('=')
                        param = m_ar1[0]
                        param = peel_str(param)
                        if param not in params:
                            command = 'echo $' + param + '\n'
                            coms.append(command)
                            params.append(param)
                elif '=' in line and 'if' not in line:
                    # There is a potential bug,
                    # when the parameter name contains 'if' it will be ignored
                    m_ar = line.split('=')
                    param = m_ar[0]
                    param = peel_str(param)
                    if param not in params:
                        command = 'echo $' + param + '\n'
                        coms.append(command)
                        params.append(param)
                else:
                    pass
        mf_exe.writelines(coms)
        mf_exe.close()
        os.chmod('mexe.sh', 0o777)
        values = os.popen('./mexe.sh').readlines()
        vals = {}
        os.remove('mexe.sh')
        if len(params) == len(values):
            for i in range(len(params)):
                vals[params[i]] = values[i].replace('\n', '')
        else:
            print("There should be something wrong with your input files!")
        return vals


def peel_str(val, query=['(', ')', '\n', ' '], replace=['', '', '', '']):
    for que, rep in zip(query, replace):
        val = val.replace(que, rep)
    return val


def num(val):
    if isinstance(val, str) and val.isnumeric():
        try:
            return int(val)
        except ValueError:
            return float(val)
    else:
        return float('nan')
