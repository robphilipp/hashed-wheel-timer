# hashed-wheel-timer

Hashed wheel timer based on [ifesdjeen hashed-wheel-timer](https://github.com/ifesdjeen/hashed-wheel-timer) with
higher time-resolution. This timer can get approximately 50 µs to 100 µs times (with some caveats).

These updates were done mainly to support signal propagation delays in my spiking neural networks, which need µs 
resolution, but can tolerate noise in the delay times.

Benchmarks and example code can be found in the [benchmark repository](https://github.com/robphilipp/hashed-wheel-timer-benchmarks).