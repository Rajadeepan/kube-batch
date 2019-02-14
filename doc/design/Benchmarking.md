## BACKGROUND

Performance comparison of KubeBatch scheduler with default scheduler. Kubemark will be used as a tool for benchmarking.
Kubemark is a performance testing tool which we use to run simulated kubernetes clusters. The primary use case is scalability 
testing, as creating simulated clusters is faster and requires less resources than creating real ones.

## OBJECTIVE

- Comparing the scheduler performance of kube-batch with the scheduler performance of the default scheduler

## DESIGN OVERVIEW

We assume that we want to benchmark a test T across two variants A(with default scheduler) and B(with kube-batch scheduler).
For the benchmarking to be meaningful,these two variants should be running in a kubemark cluster and
at identical scale (eg. both run 1k nodes).
At a high-level, the kubemark should:

- *choose the set of runs* of tests T executed on both A environment using the default scheduler and
    B environment using the kube-batch scheduler by setting "schedulerName: kube-batch" in the pod spec,
- *obtain the relevant metrics (E2e Pod startup latencies)* for the runs chosen for comparison,
- *compute the similarity* for  metric, across both the samples,

Final output of the tool will be the answer to the question performance comparison between the kube-batch scheduler and the 
default kube scheduler.


### Choosing set of metrics to compare

Kubermark give the following metrics
- pod startup latency
- api request serving latency (split by resource and verb)

In our case we would requiring <b> E2E Pod Starup Latency </b> which would include
   - The Latency for <b> scheduling the pod </b>
   - The Latency for <b> binding the pod to node </b>
