# NonBlocking
Implementation of a non-blocking dictionary.  
https://www.nuget.org/packages/NonBlocking

## Overview

NonBlocking dictionary:

- NonBlocking dictionary has the same API as ConcurrentDictionary.
- No locks are taken during any operation including Get, Add, Remove, internal resizes etc...
- While multiple threads accessing NonBlocking dictionary will help each other in operations such as table resizing, there is no dependency on such behavior. If any thread get unscheduled or delayed for whatever reason, other threads will be able to make progress independently.

## Implementation notes
Core algorithms are based on NonBlockingHashMap, written and released to the public domain by Dr. Cliff Click.
A good overview is here https://www.youtube.com/watch?v=HJ-719EGIts

Most differences of this implementation are motivated by the differences in public APIs on .Net platform:
- .Net has structs and reified generics.
- platform APIs such as Interlocked.CompareExchange have differences.
- ConcurrentDictionary API differs from a typical java dictionary.

## Tested on platforms:
* x86
* x64
* arm
* arm64

## Performance

On most operations NonBlocking dictionary is faster than ConcurrentDictionary. It is particularly faster in write-heavy scenarios.  
Read/Write oprations scale linearly with the number of active threads if hardware/OS permits.  

## Benchmarks

The test machine is  
	AMD EPYC (Rome) 64-Core Processor @ 2.44GHz	
	
	Configured as: 
       4 NUMA nodes  
       256 Logical processors.

The following sample benchmarks perform various operations on {int --> string} dictionaries and run as 64bit process.  

![Get](/../pictures//Graphs/Read.png?raw=true "Random Get")
The machine is configured as 4-node NUMA and Windows scheduler would use cores from one node, then their HT siblings, and only then use another node. Thus we see "steps".

![Write](/../pictures//Graphs/Write.png?raw=true "Random Write")
Not taking locks makes writes cheaper.

![Add](/../pictures//Graphs/Add.png?raw=true "Random Add")
When a table grows to 1000000 element we start over with a new table.  
Add/Resize scale well when no locks need to be taken, as long as the rest of the system (like GC) can keep up.





