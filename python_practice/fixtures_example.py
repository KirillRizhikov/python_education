import pytest


@pytest.fixture(scope='module')
def cur():
    print('setting up')
    db = MyDB()
    conn = db.connect('server')
    curs = conn.cursor()
    yield curs
    curs.close()
    conn.close()
    print('closing')

def test_johns_id(cur):
    id = cur.execute('Select id from ...')
    assert id ==123

def test_toms_id(cur):
    id = cur.execute('Select id from ...')
    assert id == 789