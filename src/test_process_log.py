# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from datetime import datetime

from process_log import parse_timestamp, parse_line, TopKDict, BlockList


def test_parse_timestamp():
    """Test that parse_timestamp does the same thing as datetime.strptime()
    for our format."""
    assert (parse_timestamp("01/Jul/1995:00:00:01") ==
            datetime.strptime("01/Jul/1995:00:00:01", '%d/%b/%Y:%H:%M:%S'))


def test_parse_line():
    line = ('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] '
            '"GET /history/apollo/ HTTP/1.0" 200 6245')
    host, timestamp, request, code, nbytes = parse_line(line)
    assert host == '199.72.81.55'
    assert timestamp == datetime(1995, 7, 1, 0, 0, 1)
    assert request == 'GET /history/apollo/ HTTP/1.0'
    assert code == 200
    assert nbytes == 6245


def test_parse_line_weird_quotes():
    """Test that parse_line works with unicode quotes."""
    line = ('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] '
            '“POST /login HTTP/1.0” 401 1420')
    host, timestamp, request, code, nbytes = parse_line(line)
    assert request == 'POST /login HTTP/1.0'


def test_topkdict():
    d = TopKDict(3)
    d['a'] = 1
    assert len(d) == 1
    
    d.update({'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e':5})
    assert d == {'c': 3, 'd': 4, 'e': 5}

    # check not updated for smaller values
    d['c'] = 2
    assert d['c'] == 3

    # ... but is update for larger values
    d['c'] = 4
    assert d['c'] == 4


def test_blocklist():
    blocklist = BlockList(fail_limit=2, fail_time=10., block_time=60.)

    assert blocklist.handle('a', datetime(1995, 7, 1, 0, 0, 0), False) == False
    assert blocklist.handle('b', datetime(1995, 7, 1, 0, 0, 0), False) == False
    assert blocklist.handle('c', datetime(1995, 7, 1, 0, 0, 0), False) == False
    # a succeeds 5 seconds later and should be reset, subsequent login
    # not blocked
    assert blocklist.handle('a', datetime(1995, 7, 1, 0, 0, 5), True) == False
    assert blocklist.handle('a', datetime(1995, 7, 1, 0, 0, 6), False) == False

    # b fails again, should be blocked after this
    assert blocklist.handle('b', datetime(1995, 7, 1, 0, 0, 9), False) == False
    assert blocklist.handle('b', datetime(1995, 7, 1, 0, 0, 10), True) == True
    # still blocked 60s later
    assert blocklist.handle('b', datetime(1995, 7, 1, 0, 1, 9), True) == True
    # unblocked now
    assert blocklist.handle('b', datetime(1995, 7, 1, 0, 1, 10), False) == False


