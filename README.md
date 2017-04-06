# insight-analytics-challenge

*Coding exercise for Insight*

I implemented my solution in pure Python. It requires either
Python 2.7 or 3.5+, and has no dependencies outside the Python standard
library.

There are unit tests in `src/test_process_log.py`. The tests run with pytest:
```
pytest src/test_process_log.py
```

## Solution Overview and Data Structures

For features 1 and 2 ("top hosts" and "top resources"), I use
the `Counter` class (a.k.a. a "multiset")  from the Python standard library.
This is simply a dictionary that keeps track of counts.

For feature 3, "busiest hours", I use a combination of a deque and a custom
class `TopKDict`. The deque is used to store only the timestamps occuring in
the past hour. The `TopKDict` class is a dictionary that only stores the top K
entries, sorted by value. As each timestamp "expires", the current length
of the deque is added to the dictionary.

For feature 4, "blocked requests", I have a class `BlockList` that handles
each login request. Internally, it keeps track of hosts that are currently
blocked and recent failed login times for hosts. These are both dictionaries
mapping hosts to time(s).


## Additional Feature: session statistics

It is often useful to have statistics about how long users spend on
your site at any given time, how many pages they view, etc. There are
a lot of questions one might try to answer, but as a first simple
pass, I calculate the average length of each "session".  Here, I'm
defining a session as a group of requests from a host with no gaps
of 30 minutes or more.

This is done in the class `Sessions`. It keeps track of active session
start and end times for each host in a dictionary. To reduce the
memory footprint, I occasionally go through the dictionary of active
hosts and clear ones that are no longer active. (Otherwise "current"
activity is only checked once a new request from the host comes in,
which means the dictionary includes every host ever to make a
request.) I also tried using a deque in combination with the
dictionary to track "expiring" sessions, but this ended up being
slower.


## Performance

Before adding the session statistics, the code ran on the 426 MB input
file in about a minute on my laptop:
```
$ run.sh 
4400644 lines in   59.546s: 73903 lines/s
```
Peak memory usage was about 200MB.

With the session statistics, it takes a bit longer:
```
$ run.sh 
4400644 lines in   69.781s: 63064 lines/s
```
with peak memory usage of about 250MB.

Profiling indicates that about half the time is spent in parsing the
lines of the input. I suspect that at least this could be sped up
significantly with a C extension (if necessary), as the timestamp
parsing is now done in pure Python.
