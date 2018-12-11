
Speed Based Throttling
----------------------

If throttling is enabled in User configuration then Speed based throttlign is applied.

Speed based throttling is implemented by
[PagesWriteSpeedBasedThrottle.java](PagesWriteSpeedBasedThrottle.java)

This approach estimates current number of page written and pages to be written.

<img src="https://docs.google.com/drawings/d/e/2PACX-1vT1u2fuSdIItg67J02ukUGx3cY1tc9B-eebRSa0Hu4zwzkzpJdNSmSCpRD1EmGhYTCxa-kYqSDKOt-v/pub?w=425&amp;h=589">


From source data remained estimated time of checkpoint is calculated. Then we apply this time estimation to our progress of marking pages as dirty.

<img src="https://docs.google.com/drawings/d/e/2PACX-1vTr9mhBts4rLzoqcRWOy78qPEL2UHMaJLIXGu4_1TlinbdLdtz5aGbhPMzy4uxLWup8dZdDsnZeOUxR/pub?w=441&amp;h=575">

