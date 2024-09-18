```
Operating System: macOS
CPU Information: Apple M1 Pro
Number of Available Cores: 8
Available memory: 16 GB
Elixir 1.17.1
Erlang 27.0
JIT enabled: true

Benchmark suite executing with the following configuration:
warmup: 2 s
time: 5 s
memory time: 0 ns
reduction time: 0 ns
parallel: 1
inputs: 10MB value, 1KB value, 1MB value, small value
Estimated total run time: 28 s

Benchmarking write with input 10MB value ...
Benchmarking write with input 1KB value ...
Benchmarking write with input 1MB value ...
Benchmarking write with input small value ...
Calculating statistics...
Formatting results...

##### With input 10MB value #####
Name            ips        average  deviation         median         99th %
write         87.19       11.47 ms    ±58.71%       10.38 ms       29.21 ms

##### With input 1KB value #####
Name            ips        average  deviation         median         99th %
write       60.09 K       16.64 μs   ±601.34%       12.71 μs       39.93 μs

##### With input 1MB value #####
Name            ips        average  deviation         median         99th %
write        763.78        1.31 ms   ±213.85%        0.67 ms        8.99 ms

##### With input small value #####
Name            ips        average  deviation         median         99th %
write       68.75 K       14.55 μs   ±365.06%       12.08 μs       36.75 μs
```