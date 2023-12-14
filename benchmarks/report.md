# Version: v0.1.0
## Write Single Entry

```
Operating System: macOS
CPU Information: Apple M1 Pro
Number of Available Cores: 8
Available memory: 16 GB
Elixir 1.15.7
Erlang 26.0.2

Benchmark suite executing with the following configuration:
warmup: 2 s
time: 5 s
memory time: 0 ns
reduction time: 0 ns
parallel: 1
inputs: 10MB value, 1KB value, 1MB value, small value, small value, nosync
Estimated total run time: 35 s

Benchmarking ExWal.write/2 with input 10MB value ...
323 entries written to WAL.
Benchmarking ExWal.write/2 with input 1KB value ...
4505 entries written to WAL.
Benchmarking ExWal.write/2 with input 1MB value ...
5316 entries written to WAL.
Benchmarking ExWal.write/2 with input small value ...
4613 entries written to WAL.
Benchmarking ExWal.write/2 with input small value, nosync ...
798933 entries written to WAL.

##### With input 10MB value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2         44.00       22.73 ms   ±397.22%        5.07 ms      701.06 ms

##### With input 1KB value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        658.62        1.52 ms   ±290.59%        1.30 ms        6.14 ms

##### With input 1MB value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        766.21        1.31 ms   ±159.26%        0.76 ms       12.84 ms

##### With input small value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        626.62        1.60 ms   ±287.77%        1.39 ms        5.69 ms

##### With input small value, nosync #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2      138.88 K        7.20 μs  ±1538.43%        4.17 μs       16.46 μs
```

## Batch Write

```
Operating System: macOS
CPU Information: Apple M1 Pro
Number of Available Cores: 8
Available memory: 16 GB
Elixir 1.15.7
Erlang 26.0.2

Benchmark suite executing with the following configuration:
warmup: 2 s
time: 5 s
memory time: 0 ns
reduction time: 0 ns
parallel: 1
inputs: 10MB * 10 value, 1KB * 10 value, 1MB * 10 value, small * 10 value, small * 10 value, nosync
Estimated total run time: 35 s

Benchmarking ExWal.write/2 with input 10MB * 10 value ...
410 entries written to WAL.
Benchmarking ExWal.write/2 with input 1KB * 10 value ...
46700 entries written to WAL.
Benchmarking ExWal.write/2 with input 1MB * 10 value ...
7150 entries written to WAL.
Benchmarking ExWal.write/2 with input small * 10 value ...
44240 entries written to WAL.
Benchmarking ExWal.write/2 with input small * 10 value, nosync ...
1398110 entries written to WAL.

##### With input 10MB * 10 value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2          4.30      232.62 ms   ±100.72%      107.59 ms      875.95 ms

##### With input 1KB * 10 value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        645.72        1.55 ms   ±287.03%        1.36 ms        5.17 ms

##### With input 1MB * 10 value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        101.90        9.81 ms    ±43.39%        9.59 ms       20.27 ms

##### With input small * 10 value #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2        570.03        1.75 ms   ±424.82%        1.40 ms        4.52 ms

##### With input small * 10 value, nosync #####
Name                    ips        average  deviation         median         99th %
ExWal.write/2       20.58 K       48.60 μs  ±1094.70%       32.29 μs          64 μs
```