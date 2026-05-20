# Zero-Allocation Reverse Proxy

> A high-performance, zero-allocation reverse proxy server built on .NET 8. Designed to eliminate Garbage Collector (GC) pressure and minimize latency in high-load environments.

## Executive Summary
Standard proxy solutions scale resource consumption linearly with load, introducing GC pauses that critically degrade latency. This project solves a fundamental infrastructure bottleneck: network traffic routing should be limited only by hardware throughput. By leveraging modern .NET primitives, this proxy ensures predictable, ultra-low latency response times while drastically reducing infrastructure overhead.

## Architecture & Tech Stack
* **Runtime:** .NET 8.0
* **Routing Core:** `System.IO.Pipelines` for zero-copy I/O operations.
* **Transport:** Asynchronous sockets with aggressive memory pooling (`Span<T>`, `Memory<T>`).

The system operates on a pipeline-driven architecture (`PipelineConnection`). Bytes stream directly from the ingress socket to the egress target without heap allocations or intermediary buffering.

## Deployment

*Note: Production deployments must execute in the Release configuration. Debug builds inject allocation overhead and invalidate performance metrics.*

```bash
git clone [https://github.com/Nanda070/Zero-Allocation-Reverse-Proxy.git](https://github.com/Nanda070/Zero-Allocation-Reverse-Proxy.git)
cd Zero-Allocation-Reverse-Proxy
dotnet build -c Release
dotnet run -c Release --project ZeroAllocationReverseProxy.csproj
```

## Strategic Roadmap

The current codebase delivers a highly optimized foundational transport layer. To scale into a full-fledged enterprise edge solution, the following architectural milestones are prioritized:

1. **Dynamic Upstream Management:** Integration with Service Discovery (e.g., Consul/etcd) for zero-downtime backend rotation.
2. **Zero-Overhead Telemetry:** Surfacing critical metrics (latency, RPS, error rates) to Prometheus via `System.Diagnostics.Metrics` without impacting the processing hot path.
3. **Intelligent Load Balancing:** Implementing Least Connections and EWMA (Exponentially Weighted Moving Average) for adaptive traffic distribution during partial node degradation.
4. **Connection Pooling:** Global TCP connection reuse to upstreams to eliminate TLS/TCP handshake latency.

## Governance & Contribution

This project enforces a strict **zero-allocation** policy on the request processing hot path.

Pull requests introducing heap allocations during routing will be rejected. All performance-critical changes must be backed by `BenchmarkDotNet` profiles utilizing `MemoryDiagnoser`. Code modifications must prioritize throughput and memory efficiency over syntactic convenience
