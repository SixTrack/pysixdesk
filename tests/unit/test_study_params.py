import shutil
import unittest
import itertools
from pathlib import Path
import sys
# give the test runner the import access
sys.path.insert(0, Path(__file__).parents[1].absolute())
from pysixdesk.lib.study_params import StudyParams
from pysixdesk.lib.study_params import set_property
from pysixdesk.lib.pysixdb import SixDB


class ParamsTest(unittest.TestCase):

    def setUp(self):

        # prepare a testing folder
        self.test_folder = Path('unit_test/study_params')
        self.test_folder.mkdir(parents=True, exist_ok=True)
        self.mask_file = Path(self.test_folder / 'test.mask')
        self.fort_file = Path(self.test_folder / 'test_fort.3')

        # some realistic mask file content
        mask_content = r'''NRJ= %e0 ; ! collision
I_MO=%I_MO; !-20
b_t_dist := %b_t_dist; !25 bunch separation [ns]
emit_norm := %emit_norm * 1e-6; Nb_0:=%bunch_charge;
sigt_col=%sigz; ! bunch length [m] in collision
test=%test1; test=%test2; test=%test3
'''
        # placeholders in mask_content
        self.mask_ph = set(['e0', 'I_MO', 'b_t_dist', 'emit_norm',
                            'bunch_charge', 'sigz', 'test1', 'test2', 'test3'])
        # some realistic fort.3 content
        fort_content = r'''GEOME-STRENG TITLE:%Runnam
%turnss 0 %nss %ax0s %ax1s 0 %imc
1 1 %idfor 1 %iclo6
0 0 1 1 %writebins 50000 2
        2 0. 0. %ratios 0
        %dp1
        0.
        %dp2
        %e0
        %e0
        %e0
      35640 .000347 %rfvol 0. %length %pmass %ition
%test1 %test2 %test3
%toggle_diff/DIFF
'''
        # placeholders in fort_content
        self.fort_ph = set(['Runnam', 'turnss', 'nss', 'ax0s', 'ax1s', 'imc',
                            'idfor', 'iclo6', 'writebins', 'ratios', 'dp1',
                            'dp2', 'e0', 'rfvol', 'length', 'pmass', 'ition',
                            'test1', 'test2', 'test3', 'toggle_diff/'])

        with open(self.mask_file, 'w') as m_f:
            m_f.write(mask_content)
        with open(self.fort_file, 'w') as f_f:
            f_f.write(fort_content)

    def test_placeholder_pattern(self):
        params = StudyParams(self.mask_file, fort_path=self.fort_file)
        self.assertTrue((self.mask_ph | self.fort_ph).issubset(set(params.keys())))
        self.assertEqual(set(params.madx.keys()), self.mask_ph)
        # they are not equal because of the 'CHROM' and 'chrom_eps'
        # which are not placeholders in the fort.3 but mandatory for the
        # oneturnresult file created in the preprocessing job.
        self.assertTrue(self.fort_ph < set(params.sixtrack.keys()))

    def test_oneturn(self):
        params = StudyParams(self.mask_file, fort_path=self.fort_file)
        oneturn = params.oneturn
        self.assertEqual(oneturn['turnss'], 1)
        self.assertEqual(oneturn['Runnam'], 'FirstTurn')
        self.assertEqual(oneturn['nss'], 1)

    def test_drop_none(self):
        # check that params with None as values, i.e. no defaults values and no
        # user set values through __set_item__, are removed from the params.
        params = StudyParams(self.mask_file, fort_path=self.fort_file)
        params.drop_none()
        no_values = ['test1', 'test2', 'test3']
        keys = params.keys()
        self.assertTrue([k not in keys for k in no_values])

        params = StudyParams(self.mask_file, fort_path=self.fort_file)
        params['test1'] = 1
        params.drop_none()
        no_values = ['test2', 'test3']  # these should be removed by drop_none
        with_values = ['test1']  # these should survive the drop_none
        keys = params.keys()
        self.assertTrue([k not in keys for k in no_values])
        self.assertTrue([k in keys for k in with_values])

    def test_setitem(self):
        params = StudyParams(self.mask_file, fort_path=self.fort_file)
        for k in self.mask_ph:
            params[k] = 1.23
        # check that the params were changed
        self.assertTrue(all([v == 1.23 for v in params.madx.values()]))

        # the keys in params.sixtrack in common with params.madx should also
        # have changed.
        intersect = self.mask_ph & self.fort_ph
        self.assertTrue(all([params.sixtrack[k] == 1.23 for k in intersect]))

        # setting different values in params.madx and params.madx
        params.madx['test1'] = 1
        params.sixtrack['test1'] = 0
        self.assertEqual(params.madx['test1'], 1)
        self.assertEqual(params.sixtrack['test1'], 0)

        # expected exceptions, user sets value of placeholder not found in
        # files.
        with self.assertRaises(KeyError):
            params['not_in_files'] = 123

    def test_calc_queue(self):
        params = StudyParams(self.mask_file, fort_path=self.fort_file)
        e0_init = params['e0']
        factor = 2

        @set_property('input_keys', ['e0'])
        @set_property('output_keys', ['e0_2'])
        def times(x, factor=2):
            return x*factor
        params.calc_queue.append(times)

        self.assertEqual(len(params.calc_queue), 1)
        out_dict = params.calc(factor=factor)
        self.assertTrue('e0_2' in out_dict.keys())
        self.assertTrue(out_dict['e0_2'] == e0_init * factor)
        # check that the calc_queue has been reset.
        self.assertEqual(params.calc_queue, [])

        # expected exceptions, returning more values than out_keys
        @set_property('input_keys', ['e0'])
        @set_property('output_keys', ['e0_2'])
        def times(x, factor=2):
            return x*factor, x*factor
        params.calc_queue.append(times)

        with self.assertRaises(ValueError):
            params.calc(factor=2)
        self.assertEqual(len(params.calc_queue), 1)

        # this tests to see if the output of one calcution can be used as
        # input of another.
        # reset queue
        params.calc_queue = []
        @set_property('input_keys', ['e0_2'])
        @set_property('output_keys', ['e0_4'])
        def times(x, factor=2):
            return x*factor
        params.calc_queue.append(times)

        @set_property('input_keys', ['e0_4'])
        @set_property('output_keys', ['e0_8'])
        def times(x, factor=2):
            return x*factor
        params.calc_queue.append(times)

        out_dict = params.calc(factor=2)
        self.assertTrue({'e0_4', 'e0_8'}.issubset(set(out_dict.keys())))
        self.assertTrue({'e0_2', 'e0_4', 'e0_8'}.issubset(set(params.sixtrack.keys())))

    def test_calc_queue_db(self):
        # this tests the reading data from database as calculation input. Using
        # the require function attribute.
        # initializing the test database
        db_info = {'db_type': 'sql',
                   'db_name': self.test_folder / 'data.db'}
        db = SixDB(db_info, create=True)
        db.create_table('test_table', {'x': 'int', 'y': 'int'}, key_info={})
        x_vals = [1, 2, 3, 4]
        y_vals = [5, 6, 7, 8]
        db.insertm('test_table', {'x': x_vals, 'y': y_vals})
        params = StudyParams(self.mask_file, fort_path=self.fort_file)

        # this calculation takes input from data in the test table
        @set_property('require', {'test_table': ['x', 'y']})
        @set_property('output_keys', ['xy', 'xyy'])
        def times_table(x=None, y=None):
            return x * y, x * y * y
        params.calc_queue.append(times_table)

        @set_property('input_keys', ['nss'])
        @set_property('output_keys', ['nss_2'])
        def nss_2_calc(nss):
            return nss*2
        params.calc_queue.append(nss_2_calc)

        # only run calculations which require 'test_table'
        out_dict = params.calc(get_val_db=db, require=['test_table'])
        self.assertTrue('xy' in out_dict.keys())
        expected_out = [times_table(x=i, y=j) for i, j in itertools.product(x_vals, y_vals)]

        self.assertEqual(out_dict['xy'], [i[0] for i in expected_out])
        self.assertTrue('xyy' in out_dict.keys())
        self.assertEqual(out_dict['xyy'], [i[1] for i in expected_out])
        # check that the calculation queue has been updated
        # i.e. only nss_2_calc remaining
        self.assertTrue(len(params.calc_queue) == 1)
        self.assertTrue(params.calc_queue[0] == nss_2_calc)
        # run remaining calculations
        out_dict.update(params.calc(require='all'))
        self.assertTrue('nss_2' in out_dict.keys())
        self.assertEqual(out_dict['nss_2'], nss_2_calc(params['nss']))

        print(out_dict)

    def tearDown(self):
        shutil.rmtree(self.test_folder.parent, ignore_errors=True)
