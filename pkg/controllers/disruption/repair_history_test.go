/*
Copyright The Kubernetes Authors.

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

package disruption

import (
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

var _ = Describe("RebootHistory", func() {
	It("escalates after two committed reboots", func() {
		history := NewRebootHistory()
		candidate := repairCandidateForTest("nodeclaim-uid")
		oneMinute := time.Minute
		results := []RepairResult{
			repairResultForTest("AcceleratorReady", "XID48", cloudprovider.RebootNode, time.Unix(1, 0), &oneMinute),
		}

		Expect(history.Resolve(candidate, results)).To(BeTrue())
		Expect(candidate.Action).To(Equal(cloudprovider.RebootNode))

		history.RecordCommittedReboot(candidate.NodeClaim.UID)
		Expect(history.Resolve(candidate, results)).To(BeTrue())
		Expect(candidate.Action).To(Equal(cloudprovider.RebootNode))
		Expect(candidate.RebootEscalated).To(BeFalse())

		history.RecordCommittedReboot(candidate.NodeClaim.UID)
		Expect(history.Resolve(candidate, results)).To(BeTrue())
		Expect(candidate.Action).To(Equal(cloudprovider.ReplaceNode))
		Expect(candidate.RebootEscalated).To(BeTrue())
		Expect(results[0].Action).To(Equal(cloudprovider.RebootNode))
	})

	It("does not record reboots while resolving", func() {
		history := NewRebootHistory()
		candidate := repairCandidateForTest("nodeclaim-uid")
		results := []RepairResult{
			repairResultForTest("AcceleratorReady", "XID48", cloudprovider.RebootNode, time.Unix(1, 0), nil),
		}

		for range 3 {
			Expect(history.Resolve(candidate, results)).To(BeTrue())
			Expect(candidate.Action).To(Equal(cloudprovider.RebootNode))
		}
	})

	It("keys history by NodeClaim UID", func() {
		history := NewRebootHistory()
		results := []RepairResult{
			repairResultForTest("AcceleratorReady", "XID48", cloudprovider.RebootNode, time.Unix(1, 0), nil),
		}
		original := repairCandidateForTest("original-uid")
		history.RecordCommittedReboot(original.NodeClaim.UID)
		history.RecordCommittedReboot(original.NodeClaim.UID)

		successor := repairCandidateForTest("successor-uid")
		Expect(history.Resolve(successor, results)).To(BeTrue())
		Expect(successor.Action).To(Equal(cloudprovider.RebootNode))
	})

	It("preserves replacement action and requires current evidence", func() {
		history := NewRebootHistory()
		candidate := repairCandidateForTest("nodeclaim-uid")
		history.RecordCommittedReboot(candidate.NodeClaim.UID)
		history.RecordCommittedReboot(candidate.NodeClaim.UID)

		replacement := []RepairResult{
			repairResultForTest("StorageReady", "DiskFailure", cloudprovider.ReplaceNode, time.Unix(1, 0), nil),
		}
		Expect(history.Resolve(candidate, replacement)).To(BeTrue())
		Expect(candidate.Action).To(Equal(cloudprovider.ReplaceNode))
		Expect(candidate.RebootEscalated).To(BeFalse())
		Expect(history.Resolve(candidate, nil)).To(BeFalse())
	})

	It("uses a sliding window", func() {
		clk := clocktesting.NewFakeClock(time.Unix(1, 0))
		history := newRebootHistory(clk)
		candidate := repairCandidateForTest("nodeclaim-uid")
		results := []RepairResult{
			repairResultForTest("AcceleratorReady", "XID48", cloudprovider.RebootNode, time.Unix(1, 0), nil),
		}

		history.RecordCommittedReboot(candidate.NodeClaim.UID)
		clk.Step(13 * time.Hour)
		history.RecordCommittedReboot(candidate.NodeClaim.UID)

		clk.Step(12 * time.Hour)
		Expect(history.Resolve(candidate, results)).To(BeTrue())
		Expect(candidate.Action).To(Equal(cloudprovider.RebootNode))

		history.RecordCommittedReboot(candidate.NodeClaim.UID)
		Expect(history.Resolve(candidate, results)).To(BeTrue())
		Expect(candidate.Action).To(Equal(cloudprovider.ReplaceNode))
	})

	It("bounds concurrent commits at the escalation threshold", func() {
		history := NewRebootHistory()
		candidate := repairCandidateForTest("nodeclaim-uid")
		const commits = 32

		var wg sync.WaitGroup
		start := make(chan struct{})
		for range commits {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				history.RecordCommittedReboot(candidate.NodeClaim.UID)
			}()
		}
		close(start)
		wg.Wait()

		Expect(history.recentReboots(candidate.NodeClaim.UID, history.clock.Now()).count).To(Equal(rebootsBeforeReplacement))
	})
})
