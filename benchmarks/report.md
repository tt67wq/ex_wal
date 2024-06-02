# Version: v0.1.0
----
## Write Single Entry

```
Operating System: macOS
CPU Information: Apple M1 Pro
Number of Available Cores: 8
Available memory: 16 GB
Elixir 1.16.0
Erlang 27.0
JIT enabled: true

Benchmark suite executing with the following configuration:
warmup: 2 s
time: 5 s
memory time: 0 ns
reduction time: 0 ns
parallel: 1
inputs: 10MB value, 1KB value, 1MB value, small value, small value, nosync
Estimated total run time: 35 s

Benchmarking ExWal.write/2 with input 10MB value ...
289 entries written to WAL.
Benchmarking ExWal.write/2 with input 1KB value ...
2942 entries written to WAL.
Benchmarking ExWal.write/2 with input 1MB value ...
3202 entries written to WAL.
Benchmarking ExWal.write/2 with input small value ...
2778 entries written to WAL.
Benchmarking ExWal.write/2 with input small value, nosync ...
733565 entries written to WAL.
Calculating statistics...
Formatting results...

##### With input 10MB value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2         37.77       26.48 ms   ±309.29%        8.89 ms      537.41 ms

##### With input 1KB value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        416.40        2.40 ms   ±730.73%        0.61 ms       13.20 ms

##### With input 1MB value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        451.13        2.22 ms   ±442.73%        0.81 ms       20.95 ms

##### With input small value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        336.71        2.97 ms   ±644.98%        0.78 ms       22.42 ms

##### With input small value, nosync #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2      121.82 K        8.21 μs  ±1631.18%        4.33 μs       26.63 μs
```

## Batch Write

```
Operating System: macOS
CPU Information: Apple M1 Pro
Number of Available Cores: 8
Available memory: 16 GB
Elixir 1.16.0
Erlang 27.0
JIT enabled: true

Benchmark suite executing with the following configuration:
warmup: 2 s
time: 5 s
memory time: 0 ns
reduction time: 0 ns
parallel: 1
inputs: 10MB * 10 value, 1KB * 10 value, 1MB * 10 value, small * 10 value, small * 10 value, nosync
Estimated total run time: 35 s

Benchmarking ExWal.write/2 with input 10MB * 10 value ...
300 entries written to WAL.
Benchmarking ExWal.write/2 with input 1KB * 10 value ...
27859 entries written to WAL.
Benchmarking ExWal.write/2 with input 1MB * 10 value ...
4479 entries written to WAL.
Benchmarking ExWal.write/2 with input small * 10 value ...
50069 entries written to WAL.
Benchmarking ExWal.write/2 with input small * 10 value, nosync ...
841329 entries written to WAL.
Calculating statistics...
Formatting results...

##### With input 10MB * 10 value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2          4.13      242.06 ms    ±84.51%      150.02 ms      808.29 ms

##### With input 1KB * 10 value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        440.45        2.27 ms   ±761.49%        0.58 ms       15.29 ms

##### With input 1MB * 10 value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2         56.81       17.60 ms   ±203.14%       12.10 ms      171.43 ms

##### With input small * 10 value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        734.90        1.36 ms   ±661.16%        0.56 ms        7.54 ms

##### With input small * 10 value, nosync #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2       12.06 K       82.90 μs   ±506.06%       59.21 μs      151.56 μs
```