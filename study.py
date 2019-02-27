import os
import sys
import configparser

class Study(object):

    def __init__(self, name='no name'):
        self.name = name
        self.config = configparser.ConfigParser()
        self.vals = {}
        self.mvals = {}

    def from_env_file(self, mfile, *files):
        '''Set up a study from the initial files of old version , e.g. sixdeskenv,sixenv
        '''
        self.mvals = self.parse_bash_script(mfile)
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
                print("The input file %s doesn't exist!"%a)
        f_out.close()
        val1 = self.parse_bash_script('cob_env.sh')
        os.remove('cob_env.sh')
        self.vals.update(val1)

    def prepare_mad6t_input(self, basefile, dest):
        '''Prepare the input files for madx and one turn sixtrack job'''
        if len(self.vals) == 0:
            print("You should load configure parameters at first!")
        else:
            #parameters for madx job
            self.config.read(basefile)
            madx_sec = self.config['madx']
            madx_path = self.vals['MADX_PATH']
            madx_exe = self.vals['MADX']
            madx_sec['madx_exe'] = madx_path + madx_exe
            seed_i = self.vals['istamad']
            seed_e = self.vals['iendmad']
            madx_sec['job_name'] = self.vals['LHCDescrip']
            madx_sec['corr_test'] = self.vals['CORR_TEST']
            madx_sec['fort_34'] = self.vals['fort_34']
            scan_vars = self.mvals['scan_variables'].split()
            scan_vals = {}
            for a in scan_vars:
                scan_vals[a] = self.mvals['scan_vals_'+a]

            #parameters for one turn sixtrack job
            six_sec = self.config['sixtrack']
            #six_sec['sixtrack_exe'] = self.vals['appName']
            six_sec['tunex'] = self.vals['tunex']
            six_sec['tuney'] = self.vals['tuney']
            six_sec['inttunex'] = self.vals['tunex']
            six_sec['inttuney'] = self.vals['tuney']
            six_sec['pmass'] = self.vals['pmass']
            six_sec['emit_beam'] = self.vals['emit_beam']
            six_sec['e0'] = self.vals['e0']
            six_sec['bunch_charge'] = self.vals['bunch_charge']
            six_sec['chrom_eps'] = self.vals['chrom_eps']
            six_sec['CHROM'] = self.vals['chrom']
            six_sec['chromx'] = self.vals['chromx']
            six_sec['chromy'] = self.vals['chromy']

            input_name = 'test.ini'
            with open(os.path.join(dest, input_name), 'w') as f_out:
                self.config.write(f_out)
            


    def parse_bash_script(self, mfile):
        '''parse the bash input file for the previous sixdesk'''
        mf_in = open(mfile, 'r')
        mf_lines = mf_in.readlines()
        mf_in.close()
        if os.path.isfile('mexe.sh'):
            os.remove('mexe.sh')
        mf_exe = open('mexe.sh', 'x')
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
                #There is a potential bug, 
                #when the parameter name contains 'if' it will be ignored
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
            print("These should somethings wrong with your input files!")
        return vals

def peel_str(val):
    val = val.replace('(', '')
    val = val.replace(')', '')
    val = val.replace('\n', '')
    val = val.replace(' ', '')
    return val

if __name__ == '__main__':
    test = Study()
    test.from_env_file('scan_definitions', 'sixdeskenv', 'sysenv')
    test.prepare_mad6t_input('./templates/mad6t.ini', './')

