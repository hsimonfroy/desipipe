from desipipe import FileManager
from desipipe.file_manager import BaseFileEntry, BaseFile


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
        assert fn == fn
        assert fn.link
        #print(fn, fn.get(option='my_option').filepath)
        fn.symlink(raise_error=False)
    assert len(fm.select(zrange=[1., 1.2])) == 2
    assert len(fm.select(zrange=[1., 1.2], ignore=True)) == len(fm)
    for fn in fm.select(filetype='catalog', tracer='ELG', ignore=['tracer']).iter(exclude='tracer'):
        assert str(fn.get(tracer='LRG') / 'test.npy').endswith('.npy')
    di = {}
    for fn in fm.select(filetype='catalog'):
        assert fn == fn
        print(fn.filepath, fn.parent.filepath, fn.name.filepath, fn.stem.filepath, fn.suffix)
        di[fn] = None  # to test if hashable
    for options in fm.iter_options(intersection=False):
        print(options)
    for fn1, fn2 in zip(fm.select(filetype='catalog'), fm.select(filetype='catalog')):
        assert fn2.filepath == fn1.filepath
    for fn in fm.select(filetype='catalog').iter(exclude=['field']):
        assert len(fn.options['field']) == 2
    fm.append(dict(description='added file', id='added_file', filetype='catalog', path='test.fits'))
    fm.append(MyFileEntry(dict(description='added file', id='added_file_2', filetype='catalog', path='test_{option}.fits', options={'option': ['my_option']})))
    fm.append(dict(fileentry='my_file_entry', description='added file', id='added_file_3', filetype='catalog', path='test_{option}.fits', options={'option': ['my_option_2']}))
    fn = '_tests/test_file_manager2.yaml'
    fm.save(fn)
    fm = FileManager(database=fn, environ=dict(DESIPIPEENVDIR='.'))
    for fn in fm.select(filetype='catalog', id=['added_file_2', 'added_file_3']):
        assert 'my_option_and_custom_file_entry' in fn.get(id='added_file_2').filepath
        assert 'my_option_2_and_custom_file_entry' in fn.get(id='added_file_3').filepath
    print(fm.exists())
    print(fm.exists(return_type='str'))

    file = BaseFile(path='test.fits')
    assert file.filepath

    fm = FileManager()
    fm.append(dict(description='Y1 data catalogs',
                   id='catalog_data_y1',
                   filetype='catalog',
                   path='tmp.fits',
                   options={'cut': {None: '', ('rp', 2.5): 'rpcut2.5', ('theta', 0.06): 'thetacut0.06'}}))
    assert fm.select(cut=[None])
    assert fm.select(cut=[('theta', 0.06)])
    assert fm.select(cut=('theta', 0.06))


if __name__ == '__main__':

    test_file_manager()