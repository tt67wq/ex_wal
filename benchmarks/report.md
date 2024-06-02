# Version: v0.2.3
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
754 entries written to WAL.
Benchmarking ExWal.write/2 with input 1KB value ...
4886 entries written to WAL.
Benchmarking ExWal.write/2 with input 1MB value ...
9228 entries written to WAL.
Benchmarking ExWal.write/2 with input small value ...
14167 entries written to WAL.
Benchmarking ExWal.write/2 with input small value, nosync ...
748535 entries written to WAL.
Calculating statistics...
Formatting results...

##### With input 10MB value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2         57.00       17.54 ms   ±290.31%        5.43 ms      328.60 ms

##### With input 1KB value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        609.47        1.64 ms   ±607.00%        1.27 ms        6.07 ms

##### With input 1MB value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        641.93        1.56 ms   ±265.03%        0.75 ms       26.40 ms

##### With input small value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        687.16        1.46 ms   ±629.37%        1.00 ms        8.17 ms

##### With input small value, nosync #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2      119.09 K        8.40 μs  ±1599.36%        4.38 μs       26.49 μs
ex_wal(dev*)$ mix run benchmarks/write.exs
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
748875 entries written to WAL.
Benchmarking ExWal.write/2 with input 1KB value ...
4614 entries written to WAL.
Benchmarking ExWal.write/2 with input 1MB value ...
4046 entries written to WAL.
Benchmarking ExWal.write/2 with input small value ...
4715 entries written to WAL.
Benchmarking ExWal.write/2 with input small value, nosync ...
611843 entries written to WAL.
Calculating statistics...
Formatting results...

##### With input 10MB value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2         47.50       21.05 ms   ±319.63%        5.88 ms      370.03 ms

##### With input 1KB value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        599.72        1.67 ms   ±791.85%        0.50 ms       17.14 ms

##### With input 1MB value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        519.93        1.92 ms   ±527.64%        0.81 ms       29.30 ms

##### With input small value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        646.95        1.55 ms   ±690.00%        0.62 ms        8.24 ms

##### With input small value, nosync #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2       99.18 K       10.08 μs  ±1565.42%        4.71 μs       33.21 μs
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
399 entries written to WAL.
Benchmarking ExWal.write/2 with input 1KB * 10 value ...
55419 entries written to WAL.
Benchmarking ExWal.write/2 with input 1MB * 10 value ...
4399 entries written to WAL.
Benchmarking ExWal.write/2 with input small * 10 value ...
58039 entries written to WAL.
Benchmarking ExWal.write/2 with input small * 10 value, nosync ...
1044129 entries written to WAL.
Calculating statistics...
Formatting results...

##### With input 10MB * 10 value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2          5.23      191.38 ms    ±89.61%      120.99 ms      684.45 ms

##### With input 1KB * 10 value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        785.11        1.27 ms   ±659.55%        0.53 ms        4.72 ms

##### With input 1MB * 10 value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2         52.43       19.07 ms   ±402.99%        5.30 ms      448.02 ms

##### With input small * 10 value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        852.09        1.17 ms   ±518.58%        0.56 ms        2.56 ms

##### With input small * 10 value, nosync #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2       15.05 K       66.44 μs   ±734.00%       33.75 μs      151.13 μs
```