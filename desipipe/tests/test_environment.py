from desipipe import Environment


def test_environment():
    environ = Environment('nersc-cosmodesi', command='mode swap pyrecon/main pyrecon/mpi')
    assert len(environ.command) == 2
    print(environ)


if __name__ == '__main__':

    test_environment()