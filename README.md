# NonBlocking
Implementation of a non-blocking dictionary and possibly other non-blocking data structures.

## Overview

NonBlockingDictionary:

- NonBlockingDictionary has the same API as a ConcurrentDictionary (except for construction).
- No locks are taken during any operation including Get, Add, Remove, etc...
- While multiple threads accessing NonBlockingDictionary will help each other in operations such as Resize, there is no dependency on such behavior. If any thread get unscheduled or delayed for whatever reason, other threads will be able to make progress independently.

## Performance

On most operations NonBlockingDictionary is faster than ConcurrentDictionary. 
It is particularly so in write-heavy scenarios.

## Simple benchmarks

TBD

## Implementation notes
Core algorithms are based on NonBlockingHashMap written and released to the public domain by Cliff Click.

