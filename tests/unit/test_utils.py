import unittest
import shutil
from pathlib import Path
import sys
# give the test runner the import access
pysixdesk_path = str(Path(__file__).parents[2].absolute())
sys.path.insert(0, pysixdesk_path)
from pysixdesk.lib import utils


class UtilsTest(unittest.TestCase):

    def setUp(self):
        # prepare a testing folder
        self.test_folder = Path('unit_test/utils/')
        self.test_folder.mkdir(parents=True, exist_ok=True)

        # replacement file
        self.replace_file_in = self.test_folder / 'replace_test.in'
        self.replace_file_out = self.test_folder / 'replace_test.out'
        self.contents = ['%pattern1', 'var = %pattern2;', 'var=%pattern3, var;']
        self.patterns = ['%pattern1', '%pattern2', '%pattern3']
        self.replace = [1, 2.5, 1e11]
        with open(self.replace_file_in, 'w') as f:
            f.writelines('\n'.join(self.contents))

        # concatenate file
        self.concat_file_in_1 = self.test_folder / 'concat_test.in'
        self.concat_file_in_2 = self.test_folder / 'concat_test_2.in'
        self.concat_contents_1 = [f'{i}\n' for i in range(10)]
        self.concat_contents_1 += ['ENDE\n'] + [f'{i}\n' for i in range(10, 20)]
        self.concat_contents_2 = [f'{i}\n' for i in range(5)]
        with open(self.concat_file_in_1, 'w') as f_1:
            f_1.writelines(''.join(self.concat_contents_1))
        with open(self.concat_file_in_2, 'w') as f_2:
            f_2.writelines(''.join(self.concat_contents_2))
        self.concat_file_out = self.test_folder / 'concat_test.out'

    def test_replace(self):
        utils.replace(self.patterns,
                      self.replace,
                      self.replace_file_in,
                      self.replace_file_out)
        with open(self.replace_file_out, 'r') as f:
            out = f.readlines()
        out = [l.rstrip() for l in out]
        self.assertEqual(out, ['1', 'var = 2.5;', 'var=100000000000.0, var;'])

    def test_compress_buf(self):
        # with strings
        in_str = 'qwertyuiopasdfghjklzxcvbnm_-./'
        in_str_comp = utils.compress_buf(in_str, source='str')
        in_str_decomp = utils.decompress_buf(in_str_comp, None, des='buf')
        self.assertEqual(in_str, in_str_decomp)
        # with file ...

        # with gzip ...

    def test_concatenate_files(self):
        utils.concatenate_files([self.concat_file_in_1, self.concat_file_in_2],
                                self.concat_file_out)
        with open(self.concat_file_out, 'r') as f_out:
            content = f_out.readlines()
        end_i = self.concat_contents_1.index('ENDE\n')
        out = self.concat_contents_1[:end_i] + self.concat_contents_2 + ['ENDE\n']
        self.assertSequenceEqual(content, out)

    def tearDown(self):
        # remove testing folder
        shutil.rmtree(self.test_folder.parent, ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
