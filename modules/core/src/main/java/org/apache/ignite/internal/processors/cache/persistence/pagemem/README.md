Apache Ignite Native Peristence Page Memory
-------------------------------------------
This package contains page memory implementation for case persitence is enabled.

## Loaded Pages Table

An implementation of [LoadedPagesMap.java](LoadedPagesMap.java)
(PageIdTable) manages mapping from Page ID to relative pointer map (rowAddr).

See introduction in wiki [Region Structure](https://cwiki.apache.org/confluence/display/IGNITE/Ignite+Durable+Memory+-+under+the+hood#IgniteDurableMemory-underthehood-Regionandsegmentstructure).

<img src="https://cwiki.apache.org/confluence/rest/gliffy/1.0/embeddedDiagrams/e9df3b17-1a57-487c-a842-dbb6b1062709.png">

Current implementation is [RobinHoodBackwardShiftHashMap.java](RobinHoodBackwardShiftHashMap.java)

## Throttling
Throttling is an intentional slowdown of operation in the grid to equate throughput of the storage and speed of user operations.

Throttling is implemented at physical level of operations, so it operates not with user entries, but with page memory pages.

For an introduction, please see
[wiki PagesWriteThrottling](https://cwiki.apache.org/confluence/display/IGNITE/Ignite+Persistent+Store+-+under+the+hood#IgnitePersistentStore-underthehood-PagesWriteThrottling)

There are two types of throttling implemented in Apache Ignite:
* Checkpoint buffer overflow protection.

This CP Buffer throttling is enabled by default. It is activated if CP buffer is close to being filled.
In this case, there is an exponential backoff at 2/3 when filling reached.
Since the CP buffer is being cleaned as the checkpoint progresses, this more or less behaves like trotting.

* the whole region marked dirty protection.
This type of throttling protects region segments from being completely filled by dirty pages when checkpoint progress is far from completion.

### Speed Based Throttling

If throttling is enabled in User configuration, then Speed based throttling is applied.

Speed based throttling is implemented by
[PagesWriteSpeedBasedThrottle.java](PagesWriteSpeedBasedThrottle.java)

Throttling is not active outside of [checkpoint](../checkpoint) process.
But when checkpoint is in progress speed based-throttling approach estimates the current number of pages written and pages to be written.

<img src="https://docs.google.com/drawings/d/e/2PACX-1vT1u2fuSdIItg67J02ukUGx3cY1tc9B-eebRSa0Hu4zwzkzpJdNSmSCpRD1EmGhYTCxa-kYqSDKOt-v/pub?w=425&amp;h=589">


From source data remained estimated time of checkpoint is calculated. Then we apply this time estimation to our progress of marking pages as dirty.

<img src="https://docs.google.com/drawings/d/e/2PACX-1vTr9mhBts4rLzoqcRWOy78qPEL2UHMaJLIXGu4_1TlinbdLdtz5aGbhPMzy4uxLWup8dZdDsnZeOUxR/pub?w=441&amp;h=575">

