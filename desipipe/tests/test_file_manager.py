from desipipe import FileManager


def test_file_manager():

    fm = FileManager(database='test_file_manager.yaml', environ=dict(DESIPIPEENVDIR='.'))
    fmp = fm.select(keywords='power')
    assert len(fmp) == 1
    for fn in fmp:
        print(fn, fn.get().rpath)
    fmp.db.append(dict(description='added file', id='added_file', filetype='catalog', path='test.fits'))
    fmp.db.write('_tests/test_file_manager2.yaml')


if __name__ == '__main__':

    test_file_manager()