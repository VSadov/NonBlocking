# NonBlocking
Implementation of a non-blocking dictionary and possibly other non-blocking data structures.

## Overview

NonBlockingDictionary:

- NonBlockingDictionary has the same API as ConcurrentDictionary (except for construction).
- No locks are taken during any operation including Get, Add, Remove, internal resizes etc...
- While multiple threads accessing NonBlockingDictionary will help each other in operations such as table resizing, there is no dependency on such behavior. If any thread get unscheduled or delayed for whatever reason, other threads will be able to make progress independently.

## Behavior differences compared to ConcurrentDictionary
There is a subtle difference in when keys are unreferenced after Remove. ConcurrentDictionary drops keys eagerly on Remove, while in the case of NonBlockingDictionary only the value is removed and the corresponding key is dropped lazily.  

ConcurrentDictionary performs Remove under a lock and as such it can expell both the key and the value "atomically". That is non the case for NonBlockingDictionary and thus only values are immediately removed. The corresponding dead key will remain in the dictionary and may become live again after corresponding Add, or, if still dead, "shaken off" when there is a shortage of free slots.

In a code that uses Remove and is sensitive to when keys become GC-unreachable, like if keys have finalizers or can reference large object graphs, the laziness of Remove could be a problem.

## Implementation notes
Core algorithms are based on NonBlockingHashMap, written and released to the public domain by Dr. Cliff Click.
A good overview is here https://www.youtube.com/watch?v=HJ-719EGIts

Most differences of this implementation are motivated by the differences in provided and expected APIs on .Net platform. In particular:
- .Net has structs and reified generics.
- platform APIs such as Interlocked.CompareExchange have differences.
- ConcurrentDictionary API differs from a typical java dictionary.

Memory model is assumed to be weak and honoring indirection data-dependency (should work on ARM, but not yet tested).

## Performance

On most operations NonBlocking Dictionary is faster than ConcurrentDictionary.  
It is particularly faster in write-heavy scenarios.  
NonBlocking Dictionary tends to scale linearly with number of threads if hardware permits.  

## Benchmarks

The test machine is  
	Intel(R) Xeon(R) CPU E5-2698B v3 @ 2.00GHz	Sockets:	2	Virtual processors:	32.

The benchmarks perform various operations on {int --> string} dictionaries and run as 64bit process.  
Y - number of operations in 1000000 ops per second.  
X - number of threads  

![Get](/../pictures//Graphs/Read.png?raw=true "Random Get")

![Write](/../pictures//Graphs/Write.png?raw=true "Random Write")

![Add](/../pictures//Graphs/Add.png?raw=true "Random Add")

One interesting observation - An average Add operation on a clean dictionary results in a cache miss and access to RAM. Another access is accrued on average due to double-up resizes. As a result, at some point Add can unavoidably be limited by the throughput of memory accesses. 

The limits of the hardware are more pronounced on the next chart where reads are done off a fresh table, not yet in a local CPU cache.
The scenario is dominated by uncached reads and is bounded by about 32 MOps/second.  
Since Add, on average, requires two uncached accesses, it seems to operate close to hardware limits.

![Get uncached](/../pictures//Graphs/ReadNocached.png?raw=true "Random Get fm Fresh table")




