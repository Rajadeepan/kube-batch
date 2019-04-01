/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"time"

	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"

	con "context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

const (
	MinPodStartupMeasurements = 100
	TotalPodCount             = 100
)

var _ = Describe("Job E2E Test", func() {
	It("Schedule Job", func() {
		context := initTestContext()
		defer cleanupTestContext(context)
		rep := clusterSize(context, oneCPU)

		_, pg := createJob(context, &jobSpec{
			name: "qj-1",
			tasks: []taskSpec{
				{
					img: "busybox",
					req: oneCPU,
					min: 2,
					rep: rep,
				},
			},
		})

		err := waitPodGroupReady(context, pg)
		checkError(context, err)
	})

	It("Schedule Multiple Jobs", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		rep := clusterSize(context, oneCPU)

		job := &jobSpec{
			tasks: []taskSpec{
				{
					img: "busybox",
					req: oneCPU,
					min: 2,
					rep: rep,
				},
			},
		}

		job.name = "mqj-1"
		_, pg1 := createJob(context, job)
		job.name = "mqj-2"
		_, pg2 := createJob(context, job)
		job.name = "mqj-3"
		_, pg3 := createJob(context, job)

		err := waitPodGroupReady(context, pg1)
		checkError(context, err)

		err = waitPodGroupReady(context, pg2)
		checkError(context, err)

		err = waitPodGroupReady(context, pg3)
		checkError(context, err)
	})

	It("Gang scheduling", func() {
		context := initTestContext()
		defer cleanupTestContext(context)
		rep := clusterSize(context, oneCPU)/2 + 1

		replicaset := createReplicaSet(context, "rs-1", rep, "nginx", oneCPU)
		err := waitReplicaSetReady(context, replicaset.Name)
		checkError(context, err)

		job := &jobSpec{
			name:      "gang-qj",
			namespace: "test",
			tasks: []taskSpec{
				{
					img: "busybox",
					req: oneCPU,
					min: rep,
					rep: rep,
				},
			},
		}

		_, pg := createJob(context, job)
		err = waitPodGroupPending(context, pg)
		checkError(context, err)

		err = waitPodGroupUnschedulable(context, pg)
		checkError(context, err)

		err = deleteReplicaSet(context, replicaset.Name)
		checkError(context, err)

		err = waitPodGroupReady(context, pg)
		checkError(context, err)
	})

	It("Gang scheduling: Full Occupied", func() {
		context := initTestContext()
		defer cleanupTestContext(context)
		rep := clusterSize(context, oneCPU)

		job := &jobSpec{
			namespace: "test",
			tasks: []taskSpec{
				{
					img: "nginx",
					req: oneCPU,
					min: rep,
					rep: rep,
				},
			},
		}

		job.name = "gang-fq-qj1"
		_, pg1 := createJob(context, job)
		err := waitPodGroupReady(context, pg1)
		checkError(context, err)

		job.name = "gang-fq-qj2"
		_, pg2 := createJob(context, job)
		err = waitPodGroupPending(context, pg2)
		checkError(context, err)

		err = waitPodGroupReady(context, pg1)
		checkError(context, err)
	})

	It("Preemption", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		job := &jobSpec{
			tasks: []taskSpec{
				{
					img: "nginx",
					req: slot,
					min: 1,
					rep: rep,
				},
			},
		}

		job.name = "preemptee-qj"
		_, pg1 := createJob(context, job)
		err := waitTasksReady(context, pg1, int(rep))
		checkError(context, err)

		job.name = "preemptor-qj"
		_, pg2 := createJob(context, job)
		err = waitTasksReady(context, pg1, int(rep)/2)
		checkError(context, err)

		err = waitTasksReady(context, pg2, int(rep)/2)
		checkError(context, err)
	})

	It("Multiple Preemption", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		job := &jobSpec{
			tasks: []taskSpec{
				{
					img: "nginx",
					req: slot,
					min: 1,
					rep: rep,
				},
			},
		}

		job.name = "preemptee-qj"
		_, pg1 := createJob(context, job)
		err := waitTasksReady(context, pg1, int(rep))
		checkError(context, err)

		job.name = "preemptor-qj1"
		_, pg2 := createJob(context, job)
		checkError(context, err)

		job.name = "preemptor-qj2"
		_, pg3 := createJob(context, job)
		checkError(context, err)

		err = waitTasksReady(context, pg1, int(rep)/3)
		checkError(context, err)

		err = waitTasksReady(context, pg2, int(rep)/3)
		checkError(context, err)

		err = waitTasksReady(context, pg3, int(rep)/3)
		checkError(context, err)
	})

	It("Schedule BestEffort Job", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		job := &jobSpec{
			name: "test",
			tasks: []taskSpec{
				{
					img: "nginx",
					req: slot,
					min: 2,
					rep: rep,
				},
				{
					img: "nginx",
					min: 2,
					rep: rep / 2,
				},
			},
		}

		_, pg := createJob(context, job)

		err := waitPodGroupReady(context, pg)
		checkError(context, err)
	})

	It("Statement", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		job := &jobSpec{
			namespace: "test",
			tasks: []taskSpec{
				{
					img: "nginx",
					req: slot,
					min: rep,
					rep: rep,
				},
			},
		}

		job.name = "st-qj-1"
		_, pg1 := createJob(context, job)
		err := waitPodGroupReady(context, pg1)
		checkError(context, err)

		now := time.Now()

		job.name = "st-qj-2"
		_, pg2 := createJob(context, job)
		err = waitPodGroupUnschedulable(context, pg2)
		checkError(context, err)

		// No preemption event
		evicted, err := podGroupEvicted(context, pg1, now)()
		checkError(context, err)
		Expect(evicted).NotTo(BeTrue())
	})

	It("TaskPriority", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		replicaset := createReplicaSet(context, "rs-1", rep/2, "nginx", slot)
		err := waitReplicaSetReady(context, replicaset.Name)
		checkError(context, err)

		_, pg := createJob(context, &jobSpec{
			name: "multi-pod-job",
			tasks: []taskSpec{
				{
					img: "nginx",
					pri: workerPriority,
					min: rep/2 - 1,
					rep: rep,
					req: slot,
				},
				{
					img: "nginx",
					pri: masterPriority,
					min: 1,
					rep: 1,
					req: slot,
				},
			},
		})

		expteced := map[string]int{
			masterPriority: 1,
			workerPriority: int(rep/2) - 1,
		}

		err = waitTasksReadyEx(context, pg, expteced)
		checkError(context, err)
	})

	It("Try to fit unassigned task with different resource requests in one loop", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)
		minMemberOverride := int32(1)

		replicaset := createReplicaSet(context, "rs-1", rep-1, "nginx", slot)
		err := waitReplicaSetReady(context, replicaset.Name)
		checkError(context, err)

		_, pg := createJob(context, &jobSpec{
			name: "multi-task-diff-resource-job",
			tasks: []taskSpec{
				{
					img: "nginx",
					pri: masterPriority,
					min: 1,
					rep: 1,
					req: twoCPU,
				},
				{
					img: "nginx",
					pri: workerPriority,
					min: 1,
					rep: 1,
					req: halfCPU,
				},
			},
			minMember: &minMemberOverride,
		})

		err = waitPodGroupPending(context, pg)
		checkError(context, err)

		// task_1 has been scheduled
		err = waitTasksReady(context, pg, int(minMemberOverride))
		checkError(context, err)
	})

	It("Job Priority", func() {
		context := initTestContext()
		defer cleanupTestContext(context)

		slot := oneCPU
		rep := clusterSize(context, slot)

		replicaset := createReplicaSet(context, "rs-1", rep, "nginx", slot)
		err := waitReplicaSetReady(context, replicaset.Name)
		checkError(context, err)

		job1 := &jobSpec{
			name: "pri-job-1",
			pri:  workerPriority,
			tasks: []taskSpec{
				{
					img: "nginx",
					req: oneCPU,
					min: rep/2 + 1,
					rep: rep,
				},
			},
		}

		job2 := &jobSpec{
			name: "pri-job-2",
			pri:  masterPriority,
			tasks: []taskSpec{
				{
					img: "nginx",
					req: oneCPU,
					min: rep/2 + 1,
					rep: rep,
				},
			},
		}

		createJob(context, job1)
		_, pg2 := createJob(context, job2)

		// Delete ReplicaSet
		err = deleteReplicaSet(context, replicaset.Name)
		checkError(context, err)

		err = waitPodGroupReady(context, pg2)
		checkError(context, err)
	})

	FIt("Schedule Density Job", func() {
		context := initKubemarkDensityTestContext()
		defer cleanupTestContext(context)

		_, pg := createDensityJob(context, &jobSpec{
			name: "qj-1",
			tasks: []taskSpec{
				{
					img: "busybox",
					req: smallCPU,
					min: TotalPodCount,
					rep: TotalPodCount,
				},
			},
		})

		err := waitDensityTasksReady(context, pg, TotalPodCount)
		checkError(context, err)

		nodeCount := 0
		missingMeasurements := 0
		nodes := getAllWorkerNodes(context)
		nodeCount = len(nodes)

		latencyPodsIterations := (MinPodStartupMeasurements + nodeCount - 1) / nodeCount
		By(fmt.Sprintf("Scheduling additional %d Pods to measure startup latencies", latencyPodsIterations*nodeCount))

		createTimes := make(map[string]metav1.Time, 0)
		nodeNames := make(map[string]string, 0)
		scheduleTimes := make(map[string]metav1.Time, 0)
		runTimes := make(map[string]metav1.Time, 0)
		watchTimes := make(map[string]metav1.Time, 0)

		var mutex sync.Mutex
		checkPod := func(p *v1.Pod) {
			mutex.Lock()
			defer mutex.Unlock()
			defer GinkgoRecover()

			if p.Status.Phase == v1.PodRunning {
				if _, found := watchTimes[p.Name]; !found {
					watchTimes[p.Name] = metav1.Now()
					createTimes[p.Name] = p.CreationTimestamp
					nodeNames[p.Name] = p.Spec.NodeName
					var startTime metav1.Time
					for _, cs := range p.Status.ContainerStatuses {
						if cs.State.Running != nil {
							if startTime.Before(&cs.State.Running.StartedAt) {
								startTime = cs.State.Running.StartedAt
							}
						}
					}
					if startTime != metav1.NewTime(time.Time{}) {
						runTimes[p.Name] = startTime
					} else {
						fmt.Println("Pod  is reported to be running, but none of its containers is", p.Name)
					}
				}
			}
		}

		additionalPodsPrefix := "density-latency-pod"
		stopCh := make(chan struct{})

		nsName := context.namespace
		_, controller := cache.NewInformer(
			&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					options.LabelSelector = labels.SelectorFromSet(labels.Set{"type": additionalPodsPrefix}).String()
					obj, err := context.kubeclient.CoreV1().Pods(nsName).List(options)
					return runtime.Object(obj), err
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					options.LabelSelector = labels.SelectorFromSet(labels.Set{"type": additionalPodsPrefix}).String()
					return context.kubeclient.CoreV1().Pods(nsName).Watch(options)
				},
			},
			&v1.Pod{},
			0,
			cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					p, ok := obj.(*v1.Pod)
					if !ok {
						fmt.Println("Failed to cast observed object to *v1.Pod.")
					}
					Expect(ok).To(Equal(true))
					go checkPod(p)
				},
				UpdateFunc: func(oldObj, newObj interface{}) {
					p, ok := newObj.(*v1.Pod)
					if !ok {
						fmt.Println("Failed to cast observed object to *v1.Pod.")
					}
					Expect(ok).To(Equal(true))
					go checkPod(p)
				},
			},
		)

		go controller.Run(stopCh)

		for latencyPodsIteration := 0; latencyPodsIteration < latencyPodsIterations; latencyPodsIteration++ {
			podIndexOffset := latencyPodsIteration * nodeCount
			fmt.Println("Creating  latency pods in range ", nodeCount, podIndexOffset+1, podIndexOffset+nodeCount)

			watchTimesLen := len(watchTimes)

			var wg sync.WaitGroup
			wg.Add(nodeCount)

			cpuRequest := *resource.NewMilliQuantity(1, resource.DecimalSI)
			memRequest := *resource.NewQuantity(1, resource.DecimalSI)

			rcNameToNsMap := map[string]string{}
			for i := 1; i <= nodeCount; i++ {
				name := additionalPodsPrefix + "-" + strconv.Itoa(podIndexOffset+i)
				nsName := context.namespace
				rcNameToNsMap[name] = nsName
				go createRunningPodFromRC(&wg, context, name, imageutils.GetPauseImageName(), additionalPodsPrefix, cpuRequest, memRequest)
				time.Sleep(200 * time.Millisecond)
			}
			wg.Wait()

			By("Waiting for all Pods begin observed by the watch...")
			waitTimeout := 10 * time.Minute
			fmt.Println("The value of len watchTimes and watchTimeslen, nodeCount", len(watchTimes), watchTimesLen, nodeCount)
			for start := time.Now(); len(watchTimes) < watchTimesLen+nodeCount; time.Sleep(10 * time.Second) {
				fmt.Println("The values of ", time.Since(start), waitTimeout)
				if time.Since(start) < waitTimeout {
					fmt.Println("Timeout reached waiting for all Pods being observed by the watch.")
				}
			}

			By("Removing additional replication controllers")
			deleteRC := func(i int) {
				defer GinkgoRecover()
				name := additionalPodsPrefix + "-" + strconv.Itoa(podIndexOffset+i+1)
				deleteReplicationController(context, name)
				fmt.Println(name)
			}
			workqueue.ParallelizeUntil(con.TODO(), 25, nodeCount, deleteRC)
		}
		close(stopCh)

		nsName = context.namespace
		fmt.Println("The namespace is ", nsName)
		time.Sleep(10 * time.Minute)
		selector := fields.Set{
			"involvedObject.kind":      "Pod",
			"involvedObject.namespace": nsName,
			"source":                   "kube-batch",
		}.AsSelector().String()
		options := metav1.ListOptions{FieldSelector: selector}
		schedEvents, _ := context.kubeclient.CoreV1().Events(nsName).List(options)
		for k := range createTimes {
			for _, event := range schedEvents.Items {
				if event.InvolvedObject.Name == k {
					scheduleTimes[k] = event.FirstTimestamp
					break
				}
			}
		}

		scheduleLag := make([]PodLatencyData, 0)
		startupLag := make([]PodLatencyData, 0)
		watchLag := make([]PodLatencyData, 0)
		schedToWatchLag := make([]PodLatencyData, 0)
		e2eLag := make([]PodLatencyData, 0)

		for name, create := range createTimes {
			fmt.Println(create)
			sched, ok := scheduleTimes[name]
			if !ok {
				fmt.Println(sched)
				missingMeasurements++
			}
			run, ok := runTimes[name]
			if !ok {
				fmt.Println("Failed to find run time for", name)
				missingMeasurements++
			}
			watch, ok := watchTimes[name]
			if !ok {
				fmt.Println("Failed to find watch time for", name)
				missingMeasurements++
			}
			node, ok := nodeNames[name]
			if !ok {
				fmt.Println("Failed to find node for", name)
				missingMeasurements++
			}
			scheduleLag = append(scheduleLag, PodLatencyData{Name: name, Node: node, Latency: sched.Time.Sub(create.Time)})
			startupLag = append(startupLag, PodLatencyData{Name: name, Node: node, Latency: run.Time.Sub(sched.Time)})
			watchLag = append(watchLag, PodLatencyData{Name: name, Node: node, Latency: watch.Time.Sub(run.Time)})
			schedToWatchLag = append(schedToWatchLag, PodLatencyData{Name: name, Node: node, Latency: watch.Time.Sub(sched.Time)})
			e2eLag = append(e2eLag, PodLatencyData{Name: name, Node: node, Latency: watch.Time.Sub(create.Time)})
		}

		sort.Sort(LatencySlice(scheduleLag))
		sort.Sort(LatencySlice(startupLag))
		sort.Sort(LatencySlice(watchLag))
		sort.Sort(LatencySlice(schedToWatchLag))
		sort.Sort(LatencySlice(e2eLag))

		PrintLatencies(scheduleLag, "worst create-to-schedule latencies")
		PrintLatencies(startupLag, "worst schedule-to-run latencies")
		PrintLatencies(watchLag, "worst run-to-watch latencies")
		PrintLatencies(schedToWatchLag, "worst schedule-to-watch latencies")
		PrintLatencies(e2eLag, "worst e2e latencies")

		//// Capture latency metrics related to pod-startup.
		podStartupLatency := &PodStartupLatency{
			CreateToScheduleLatency: ExtractLatencyMetrics(scheduleLag),
			ScheduleToRunLatency:    ExtractLatencyMetrics(startupLag),
			RunToWatchLatency:       ExtractLatencyMetrics(watchLag),
			ScheduleToWatchLatency:  ExtractLatencyMetrics(schedToWatchLag),
			E2ELatency:              ExtractLatencyMetrics(e2eLag),
		}

		fmt.Println(podStartupLatency.PrintHumanReadable())
		fmt.Println(podStartupLatency.PrintJSON())

		dir, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(dir)

		filePath := path.Join(dir, "MetricsForE2ESuite_"+time.Now().Format(time.RFC3339)+".json")
		if err := ioutil.WriteFile(filePath, []byte(podStartupLatency.PrintJSON()), 0644); err != nil {
			fmt.Errorf("error writing to %q: %v", filePath, err)
		}

	})
})
