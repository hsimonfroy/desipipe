from desipipe import FileManager


def test_file_manager():

    fm = FileManager(database='test_file_manager.yaml', environ=dict(DESIPIPEENVDIR='.'))
    fmp = fm.select(keywords='power', zrange=[1., 1.2])
    assert len(fmp) == 1
    assert len(fmp.filepaths) == 6
    for fn in fmp:
        print(fn, fn.get(option='my_option').filepath)
    fmp.append(dict(description='added file', id='added_file', filetype='catalog', path='test.fits'))
    fmp.write('_tests/test_file_manager2.yaml')


if __name__ == '__main__':

    test_file_manager()