# version history

## Version 0.0.4 (accuracy improvement)
1.  Wait strategy now returns a future when call to `waitUntil(...)` is called, and the wait
    loop is run on its own thread. This reduces the variance of the periodic schedules, and
    improves the accuracy of the one-shot timer somewhat.

## Version 0.0.3 (renamed packages)
1.  Renamed the package `com.piggy` to `com.digitalcipher`.
2.  Updated the docs.

## Version 0.0.1 (initial release)
1.  Initial release of the hashed-wheel-timer