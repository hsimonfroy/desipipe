from desipipe import Environment


def test_environment():
    environ = Environment('nersc-cosmodesi')
    print(environ)


if __name__ == '__main__':

    test_environment()