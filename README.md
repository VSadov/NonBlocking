# NonBlocking
Implementation of a non-blocking dictionary and possibly other non-blocking data structures.

## Overview

NonBlockingDictionary:

- NonBlockingDictionary has the same API as a ConcurrentDictionary (except for construction).
- No locks are taken during any operation including Get, Add, Remove, internal resizes etc...
- While multiple threads accessing NonBlockingDictionary will help each other in operations such as table resizing, there is no dependency on such behavior. If any thread get unscheduled or delayed for whatever reason, other threads will be able to make progress independently.

## Performance

On most operations NonBlockingDictionary is faster than ConcurrentDictionary.
It is particularly faster in write-heavy scenarios.

## Simple benchmarks

TBD

## Implementation notes
Core algorithms are based on NonBlockingHashMap written and released to the public domain by Cliff Click.
A good overview is here https://www.youtube.com/watch?v=HJ-719EGIts

Most differences of this implementation are motivated by the differences in provided and expected APIs on .Net platform. In particular:
- .Net has structs and reified generics.
- platform APIs such as Interlocked.CompareExchange have differences.
- ConcurrentDictionary API differs from a typical java dictionary

Memory model is assumed to be weak with data-dependency ordering (should work on ARM, but not yet tested).


