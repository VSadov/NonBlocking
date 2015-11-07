# NonBlocking
Implementation of a non-blocking dictionary and possibly other non-blocking data structures.

## Overview

NonBlockingDictionary:

- NonBlockingDictionary has the same API as ConcurrentDictionary (except for construction).
- No locks are taken during any operation including Get, Add, Remove, internal resizes etc...
- While multiple threads accessing NonBlockingDictionary will help each other in operations such as table resizing, there is no dependency on such behavior. If any thread get unscheduled or delayed for whatever reason, other threads will be able to make progress independently.

## Performance

On most operations NonBlockingDictionary is faster than ConcurrentDictionary.  
It is particularly faster in write-heavy scenarios.

Very simple "run in a loop and measure" benchmarks -    
Result is in milliseconds, smaller is better.  
64bit runs on Intel(R) CPU E31230 @3.20GHz, 8 logical cores (HT) 

* Single-threaded sequential operations on an int->string dictionary. 

|Collection \ Operation|Get|Add/Remove|
|----------|---|---|
|Dictionary|3393 (0.82x)|3227 (0.37x)|
|NonBlockingDictionary|4124 (1.00x)|8704 (1.00x)|
|ConcurrentDictionary|5413 (1.31x)|12181 (1.40x)|

* Concurrent Parallel.For operations on an int->string dictionary. 

|Collection \ Operation|Get|Add/Remove|
|----------|---|---|
|NonBlockingDictionary|1932 (1.00x)|3068 (1.00x)|
|ConcurrentDictionary|2157 (1.12x)|4034 (1.31x)|

* Concurrent GetOrAdd with a trivial Func on a partially cleared int->string dictionary.  
  % is the ratio of items removed that will end up needing Func eval at GetOrAdd stage.


|Collection \ "removed" ratio|0%|33%|66%|100%|
|----------|---|---|---|---|
|NonBlockingDictionary|1112 (1.00x)|1875 (1.00x)|2520 (1.00x)|3070 (1.00x)|
|ConcurrentDictionary|1132 (1.02x)|2331 (1.24x)|3361 (1.33x)|4608 (1.50x)|


## Implementation notes
Core algorithms are based on NonBlockingHashMap, written and released to the public domain by Dr. Cliff Click.
A good overview is here https://www.youtube.com/watch?v=HJ-719EGIts

Most differences of this implementation are motivated by the differences in provided and expected APIs on .Net platform. In particular:
- .Net has structs and reified generics.
- platform APIs such as Interlocked.CompareExchange have differences.
- ConcurrentDictionary API differs from a typical java dictionary.

Memory model is assumed to be weak and honoring indirection data-dependency (should work on ARM, but not yet tested).


