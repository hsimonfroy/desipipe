from desipipe.io import ChainFile, ProfilesFile


def test_io():
    from desilike.samples import Chain, Profiles
    chain = Chain()
    fi = ChainFile('_tests/test.npy')
    fi.save(chain)
    fi.load()

    profiles = Profiles()
    fi = ProfilesFile('_tests/test.npy')
    fi.save(profiles)
    fi.load()


if __name__ == '__main__':

    test_io()