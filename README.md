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

|Collection|Get|Add/Remove|
|----------|---|---|
|Dictionary|3402 (0.82x)|3231 (0.36x)|
|NonBlockingDictionary|4133|8771|
|ConcurrentDictionary|5356 (1.30x)|12262 (1.40x)|

* Concurrent Parallel.For operations on an int->string dictionary. 

|Collection|Get|Add/Remove|
|----------|---|---|
|NonBlockingDictionary|1993|3449|
|ConcurrentDictionary|2276 (1.14x)|4388 (1.27x)|

## Implementation notes
Core algorithms are based on NonBlockingHashMap, written and released to the public domain by Dr. Cliff Click.
A good overview is here https://www.youtube.com/watch?v=HJ-719EGIts

Most differences of this implementation are motivated by the differences in provided and expected APIs on .Net platform. In particular:
- .Net has structs and reified generics.
- platform APIs such as Interlocked.CompareExchange have differences.
- ConcurrentDictionary API differs from a typical java dictionary.

Memory model is assumed to be weak with data-dependency ordering (should work on ARM, but not yet tested).


