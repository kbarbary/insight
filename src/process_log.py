#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals

from argparse import ArgumentParser
import codecs  # use explicit codec decodes to support utf-8 on Python 2
from collections import Counter, deque
from datetime import datetime, timedelta
from operator import itemgetter
import re
from time import time as clock

# Log line format.
LINE_PATTERN = re.compile(r'(.*) - - \[(.+) -0400\] ["“](.+)["”] (.+) (.+)')

MONTHS = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
          'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}


def parse_timestamp(value):
    """Parse a timestamp formatted as '%d/%b/%Y:%H:%M:%S'.

    This is a faster version of
    datetime.strptime(value, '%d/%b/%Y:%H:%M:%S'), which turns out to
    be quite slow.

    Parameters
    ----------
    value : str
        Timestamp formatted exactly as '%d/%b/%Y:%H:%M:%S'.

    Returns
    -------
    timestamp : datetime.datetime
    """
    return datetime(int(value[7:11]), # %Y
                    MONTHS[value[3:6]], # %m
                    int(value[0:2]), # %d
                    int(value[12:14]), # %H
                    int(value[15:17]), # %M
                    int(value[18:20])) # %s


def parse_line(line):
    """Parse a line of the log file.

    Parameters
    ----------
    line : str
        Single line of the log file.

    Returns
    -------
    host : str
        Host making request.
    timestamp : datetime
        Time of request.
    request : str
        The HTTP request (no quotes).
    code : int
        The HTTP response code.
    nbytes : int
        Number of bytes.
    """
    match = LINE_PATTERN.match(line)
    if not match:
        raise ValueError("unexpected line format: {!r}".format(line))

    host, raw_timestamp, request, code, nbytes = match.groups()
    timestamp = parse_timestamp(raw_timestamp)
    code = int(code)
    nbytes = 0 if nbytes == '-' else int(nbytes)

    return host, timestamp, request, code, nbytes


class TopKDict(dict):
    """Dictionary that only stores up to k entries, sorted by value.

    To take precedence over existing values, new values must be strictly
    greater (``>``). This applies to d[key] = value for an existing key
    in the dictionary as well.

    Examples
    --------
    
    ```
    >>> d = TopKDict(2)
    >>> d.update({'a': 1, 'b': 2, 'c': 3, 'd': 4})
    >>> d
    {'c': 3, 'd': 4}
    ```

    Dictionary is unchanged for smaller values of existing keys:

    ```
    >>> d['c'] = 2
    >>> d
    {'c': 3, 'd': 4}
    ```

    ... But is updated for larger values:

    ```
    >>> d['c'] = 5
    >>> d
    {'c': 5, 'd': 4}
    ```
    """
    def __init__(self, k):
        self.k = k
        self._sorted_keys = []  # up to length k list of ordered keys.
        dict.__init__(self)

    def __setitem__(self, key, value):
        # if already there, only update if new value is greater.
        if key in self:
            if self[key] < value:
                dict.__setitem__(self, key, value)

        # always add if length is less than k
        elif len(self) < self.k:
            dict.__setitem__(self, key, value)
            self._sorted_keys.append(key)

        # otherwise, only add if value is greater than smallest existing. 
        elif value > self[self._sorted_keys[-1]]:
            del self[self._sorted_keys[-1]]
            dict.__setitem__(self, key, value)
            self._sorted_keys[-1] = key
        else:
            return

        # If we got this far, we updated the dictionary and need to re-sort.
        self._sorted_keys.sort(key=lambda x: self[x], reverse=True)

    def update(self, other):
        for k, v in other.items():
            self[k] = v


class BlockList(object):
    """Ledger of recent login failures and blocked hosts.

    This class handles all attempted logins, internally keeping track
    of failures in order to determine whether each new login should be
    blocked or not. The main method is ``BlockList.handle()``.

    Parameters
    ----------
    fail_limit : int
        Number of login failures in ``fail_time`` seconds that will trigger
        blocking.
    fail_time : float
        Time interval (in seconds) over which ``fail_limit`` login failures
        will trigger blocking.
    block_time : float
        Time interval (in seconds) over which to block attempted logins once
        blocking has been triggered for a particular host.
    """
    
    def __init__(self, fail_limit=3, fail_time=20.0, block_time=300.0):
        self.fail_limit = fail_limit
        self.fail_time = fail_time
        self.block_time = block_time
        self._blocking = {}  # maps currently blocked hosts to time blocked
        self._failures = {}  # maps not-yet-blocked hosts to a list of failed
                             # login times
                             
    def handle(self, host, time, success):
        """Handle login request and determine whether request is blocked.

        Parameters
        ----------
        host : str
            Name of host attempting login.
        time : datetime.datetime
            Timestamp of request.
        success : bool
            Whether or not login was successful.
        
        Returns
        -------
        is_blocked : bool
            True if request should be blocked, false otherwise.
        """

        # If we're currently blocking this host, check if the time has
        # expired. If so, remove from block list and carry on checking
        # the login. Otherwise, we *are* still blocking and nothing else
        # needs to be checked.
        if host in self._blocking:
            time_since_block = (time - self._blocking[host]).total_seconds()
            if time_since_block > self.block_time:
                del self._blocking[host]
            else:
                return True

        # We're not blocking. If login succeeds, clear previous failures
        # and return.
        if success:
            if host in self._failures:
                del self._failures[host]
            return False

        # If we get here, this is a failed login that is not blocked.
        # We'll log the failure and check if it is over the limit.
        if host not in self._failures:
            self._failures[host] = []
        failures = self._failures[host]
        
        # Remove failure times not in the last 20 seconds.
        while (len(failures) > 0 and
               (time - failures[0]).total_seconds() > self.fail_time):
            failures.pop(0)

        # Add new failure and check if we are over the limit
        failures.append(time)
        if len(failures) >= self.fail_limit:
            self._blocking[host] = time
            del self._failures[host]

        return False


class Sessions(object):
    """Log of user sessions: request(s) grouped closely in time.
    
    Parameters
    ----------
    inactive_limit : float, optional
        Maximum number of seconds between requests in the same session
        (exclusive).
    """
    def __init__(self, inactive_limit=1800.):
        self.inactive_limit = inactive_limit
        self._active_sessions = {}  # maps host to [start_time, end_time]

        self._clear_interval = 100000
        self._logs_since_cleared = 0
        # running totals
        self._nsessions = 0
        self._total_length = 0. 
        self._max_length = 0

    def _clear_inactive(self, current_time):
        inactive = []
        for host in self._active_sessions:
            duration = self._active_sessions[host]
            idle_time = (current_time - duration[1]).total_seconds()
            if (idle_time >= self.inactive_limit):
                self._save_session(duration)
                inactive.append(host)

        for host in inactive:
            del self._active_sessions[host]

    def _clear_all(self):
        for host in self._active_sessions:
            self._save_session(self._active_sessions[host])
        self._active_sessions = {}

    def log(self, host, time):
        """Log a request.

        Parameters
        ----------
        host : str
            Name of host.
        time : datetime.datetime
            Time of request.
        """
        # purely as a memory optimization, we occasionally clear inactive
        # sessions
        self._logs_since_cleared += 1
        if self._logs_since_cleared > self._clear_interval:
            self._clear_inactive(time)
            self._logs_since_cleared = 0

        if host not in self._active_sessions:
            self._active_sessions[host] = [time, time]
            return

        duration = self._active_sessions[host]
        idle_time = (time - duration[1]).total_seconds()
        if  idle_time >= self.inactive_limit:
            self._save_session(duration)
            duration[0] = duration[1] = time
        else:
            duration[1] = time

    def _save_session(self, duration):
        self._nsessions += 1
        length = (duration[1] - duration[0]).total_seconds()
        self._total_length += length
        if length > self._max_length:
            self._max_length = length

    def summary_statistics(self):
        """Write statistics to the open file `out_file`."""
        self._clear_all()
        return (self._nsessions,
                self._total_length / self._nsessions,
                self._max_length)

        
def main(argv=None):
    parser = ArgumentParser(prog="process_log.py",
                            description="Process a log file.")
    parser.add_argument("log", help="Filename of log file to process.")
    parser.add_argument("hosts",
                        help="Output list of top hosts making requests.")
    parser.add_argument("hours", help="Output list of top busiest hours.")
    parser.add_argument("resources",
                        help="Output list of top resources by bandwidth.")
    parser.add_argument("blocked", help="Output log of blocked requests.")
    parser.add_argument("sessions", help="Statistics on session lengths.")
    args = parser.parse_args(argv)

    # Initialize structures that will hold running totals and counts.
    host_requests = Counter()
    resource_bytes = Counter()
    recent_timestamps = deque()
    top_hours = TopKDict(10)
    block_list = BlockList(fail_limit=3, fail_time=20.0, block_time=300.0)
    sessions = Sessions(inactive_limit=1800.0)
    
    # Open files: we read the log file in binary mode and decode to unicode
    # separately in order to get proper line splits (e.g., avoid splitting on
    # unicode FORMFEED characters that might be in the request).
    # We open the output file of blocked hosts using `codecs` in order to
    # support Python 2.
    log_file = open(args.log, 'rb')
    block_file = codecs.open(args.blocked, 'w', encoding='utf-8')

    # For measuring processing speed.
    nlines = 0
    t0 = clock()

    for rawline in log_file:
        line = codecs.decode(rawline, 'utf-8', 'replace') 
        nlines += 1

        host, time, request, code, nbytes = parse_line(line)
        
        host_requests[host] += 1  # count host requests

        # Update our queue of recent timestamps (in the last hour).
        # For each hour that has now ended, add number of requests in that
        # hour to our dictionary of "top hours."
        while (len(recent_timestamps) > 0 and
               (time - recent_timestamps[0]).total_seconds() > 3600.0):
            n = len(recent_timestamps)
            hour_start = recent_timestamps.popleft()
            top_hours[hour_start] = n

        recent_timestamps.append(time)

        sessions.log(host, time)

        # The other analyses below require us to be able to
        # parse the request and extract a resource. We'll skip these
        # if the request is not well-formatted according to our
        # webserver.
        if code == 400:
            continue
        try:
            words = request.split()
            method = words[0]
            resource = words[1]
        except:
            continue
            
        # Count resources requested.
        resource_bytes[resource] += nbytes

        # Handle logins; decide whether to block.
        if method == 'POST' and resource == '/login':
            is_blocked = block_list.handle(host, time, code == 200)
            if is_blocked:
                block_file.write(line)

    runtime = clock() - t0
    print("{:d} lines in {:8.3f}s: {:.0f} lines/s"
          .format(nlines, runtime, nlines / runtime))

    log_file.close()
    block_file.close()
    
    # Clear the queue of recent timestamps.
    while len(recent_timestamps) > 0:
        n = len(recent_timestamps)
        hour_start = recent_timestamps.popleft()
        top_hours[hour_start] = n

    # Output host requests.
    host_requests_tuples = sorted(host_requests.items(), key=itemgetter(0))
    host_requests_tuples.sort(key=itemgetter(1), reverse=True)
    with codecs.open(args.hosts, 'w', encoding='utf-8') as host_file:
        for host, count in host_requests_tuples[0:10]:
            host_file.write("{:s},{:d}\n".format(host, count))

    # Output resource requests.
    resource_bytes_tuples = sorted(resource_bytes.items(), key=itemgetter(0))
    resource_bytes_tuples.sort(key=itemgetter(1), reverse=True)
    with codecs.open(args.resources, 'w', encoding='utf-8') as resource_file:
        for name, _ in resource_bytes_tuples[0:10]:
            resource_file.write("{:s}\n".format(name))

    # Output top hours.
    sorted_top_hours = sorted(top_hours.items(), key=lambda t: t[1],
                              reverse=True)
    with codecs.open(args.hours, 'w', encoding='utf-8') as hours_file:
        for timestamp, counts in sorted_top_hours:
            hours_file.write("{:s} -0400,{:d}\n".format(
                timestamp.strftime("%d/%b/%Y:%H:%M:%S"), counts))

    # Output session statistics
    with codecs.open(args.sessions, 'w', encoding='utf-8') as sessions_file:
        n, avg, maxlen = sessions.summary_statistics()
        sessions_file.write("total sessions: {:d}\n".format(n))
        sessions_file.write("average session: {:.1f}\n".format(avg))
        sessions_file.write("maximum session: {:.1f}\n".format(maxlen))

if __name__ == '__main__':
    main()
