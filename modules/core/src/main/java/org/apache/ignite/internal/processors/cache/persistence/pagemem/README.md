Apache Ignite Native Peristence Page Memory
-------------------------------------------
This package contains page memory implementation for case persitence is enabled.

Speed Based Throttling
----------------------
For an introduction, please see
[wiki PagesWriteThrottling](https://cwiki.apache.org/confluence/display/IGNITE/Ignite+Persistent+Store+-+under+the+hood#IgnitePersistentStore-underthehood-PagesWriteThrottling)

If throttling is enabled in User configuration, then Speed based throttling is applied.

Speed based throttling is implemented by
[PagesWriteSpeedBasedThrottle.java](PagesWriteSpeedBasedThrottle.java)

Throttling is not active outside of [checkpoint](../checkpoint) process.
But when checkpoint is in progress speed based-throttling approach estimates the current number of pages written and pages to be written.

<img src="https://docs.google.com/drawings/d/e/2PACX-1vT1u2fuSdIItg67J02ukUGx3cY1tc9B-eebRSa0Hu4zwzkzpJdNSmSCpRD1EmGhYTCxa-kYqSDKOt-v/pub?w=425&amp;h=589">


From source data remained estimated time of checkpoint is calculated. Then we apply this time estimation to our progress of marking pages as dirty.

<img src="https://docs.google.com/drawings/d/e/2PACX-1vTr9mhBts4rLzoqcRWOy78qPEL2UHMaJLIXGu4_1TlinbdLdtz5aGbhPMzy4uxLWup8dZdDsnZeOUxR/pub?w=441&amp;h=575">

