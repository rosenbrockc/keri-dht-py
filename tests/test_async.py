import pytest
import logging
import numpy as np
from operator import itemgetter
from datetime import datetime
import pickle

from keridht.utility import relpath

log = logging.getLogger(__name__)


def aggregate_timing(timing):
    """Aggregates all the timing entries from the asynchronous running
    of the DHT client requests.
    """
    apart = {}
    for key, times in timing.items():
        puts, gets = {}, {}
        for l, t in times:
            method, client, direction = l.split(':')
            if method == "PUT":
                if client not in puts:
                    puts[client] = []
                puts[client].append((direction, t))
            elif method == "GET":
                if client not in gets:
                    gets[client] = []
                gets[client].append((direction, t))        

        apart[key] = {}
        plist, glist = None, None
        for client in puts:
            plist = sorted(puts[client], key=itemgetter(1))
            glist = sorted(gets.get(client,[]), key=itemgetter(1))    
            apart[key][client] = (plist, glist)
            
    return apart


def _calculate_delta(slist):
    """Calculates a set of time deltas from the given sorted list
    output by :meth:`aggregate_timing`.
    
    Args:
        slist (list): of sorted `(direction, time)` tuples.
    """
    deltas = []
    ins, outs = [], []
    for direction, t in slist:
        if direction == 'i':
            ins.append(t)
        else:
            outs.append(t)
            
        for tstart, tend in zip(sorted(outs), sorted(ins)):
            deltas.append(tend-tstart)
            
    return deltas


def get_deltas(apart):
    """Extracts the timing deltas across all completed operations in the DHT.
    
    Args:
        apart (dict): output of :meth:`aggregate_timing`.
    """
    ptimes, gtimes = [], []
    for key, times in apart.items():
        for client, (plist, glist) in times.items():
            ptimes.extend(_calculate_delta(plist))
            gtimes.extend(_calculate_delta(glist))
            
    return (ptimes, gtimes)


def is_completed(aggregated):
    """Determine whether all DHT operations completed. This means we have
    timing information for both the outgoing and incoming requests.
    """
    stats = {}
    ok, nok = 0, 0
    for k, agg in aggregated.items():
        _puts, _gets = [], []
        for client, (puts, gets) in agg.items():
            ins, outs = [], []
            for kind, t in puts:
                if kind == 'i':
                    ins.append(t)
                else:
                    outs.append(t)

            matched = len(ins) == len(outs)
            _puts.append(matched)
            if matched: ok += 1 
            else: nok += 1

            ins, outs = [], []
            for kind, t in gets:
                if kind == 'i':
                    ins.append(t)
                else:
                    outs.append(t)

            matched = len(ins) == len(outs)
            _gets.append(matched)
            if matched: ok += 1 
            else: nok += 1

        stats[k] = (_puts, _gets)
        
    return {
        "details": stats,
        "matched": ok,
        "unmatched": nok,
        "total": ok + nok
    }


def test_async(client_tester, server):
    """Allows all the text fixtures to run through all of their tests and interactions.
    """
    log.debug(f"Running tests using fixture {client_tester} against {server}.")
    timing = client_tester.timing
    aggtime = aggregate_timing(timing)
    deltas = get_deltas(aggtime)
    statistics = {
        "completion": is_completed(aggtime),
        "deltas": deltas,
        "put": (len(deltas[0]), np.mean(deltas[0]), np.std(deltas[0])),
        "get": (len(deltas[1]), np.mean(deltas[1]), np.std(deltas[1])),
        "opcount": len(aggtime)
    }

    ts = int(datetime.utcnow().timestamp())
    target = relpath(f"../stats/{ts}.pkl")
    with open(target, 'wb') as f:
        log.info(f"Dumping statistics to binary pickle {target}.")
        pickle.dump(statistics, f)