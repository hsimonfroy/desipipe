import uuid
import pickle
import hashlib


def func(a, b, c=2):
    toret = a * b * c
    return toret


def test_uid():

    uid = pickle.dumps((func, (1, 2), {'c': 3}))
    hex = hashlib.md5(uid).hexdigest()
    id = uuid.UUID(hex=hex)
    print(id)


if __name__ == '__main__':

    test_uid()