from desipipe import FileManager
from desipipe.file_manager import BaseFileEntry


class MyFileEntry(BaseFileEntry):

    name = 'my_file_entry'

    def _get_file(self, options, foptions=None):
        """Return :class:`BaseFile` given input options."""
        if 'option' in foptions:
            foptions['option'] = foptions['option'] + '_and_custom_file_entry'
        return super(MyFileEntry, self)._get_file(options, foptions=foptions)


def test_file_manager():

    fm = FileManager(database='test_file_manager.yaml', environ=dict(DESIPIPEENVDIR='.'))
    fmp = fm.select(keywords='power', zrange=[1., 1.2])
    assert len(fmp) == 1
    assert len(fmp.filepaths) == 6
    for fn in fmp:
        print(fn, fn.get(option='my_option').filepath)
    for fn in fm.select(filetype='catalog'):
        print(fn.filepath)
    for fn in fm.select(filetype='catalog').iter(exclude=['field']):
        assert len(fn.options['field']) == 2
    fmp.append(dict(description='added file', id='added_file', filetype='catalog', path='test.fits'))
    fmp.append(MyFileEntry(dict(description='added file', id='added_file_2', filetype='catalog', path='test_{option}.fits', options={'option': ['my_option']})))
    fmp.append(dict(fileentry='my_file_entry', description='added file', id='added_file_3', filetype='catalog', path='test_{option}.fits', options={'option': ['my_option_2']}))
    fn = '_tests/test_file_manager2.yaml'
    fmp.write(fn)
    fm = FileManager(database=fn, environ=dict(DESIPIPEENVDIR='.'))
    for fn in fm.select(filetype='catalog', id=['added_file_2', 'added_file_3']):
        assert 'my_option_and_custom_file_entry' in fn.get(id='added_file_2').filepath
        assert 'my_option_2_and_custom_file_entry' in fn.get(id='added_file_3').filepath


if __name__ == '__main__':

    test_file_manager()