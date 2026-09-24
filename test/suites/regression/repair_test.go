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

package integration_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kwokcloudprovider "sigs.k8s.io/karpenter/kwok/cloudprovider"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/test/pkg/environment/common"
)

const (
	repairNodeCount  = 6
	repairToleration = 30 * time.Second
	repairDrainBound = 45 * time.Second
)

// Repair uses a KWOK-only condition that remains independent from Ready. This keeps the node schedulable while repair
// evaluates a durable, provider-supported fault and avoids races with KWOK's heartbeat stage.
var _ = Describe("Repair", func() {
	BeforeEach(func() {
		if !env.IsDefaultNodeClassKWOK() {
			Skip("repair regression coverage requires the deterministic KWOK repair condition")
		}
	})

	injectFault := func(node *corev1.Node, eligible bool) metav1.Time {
		GinkgoHelper()
		transitionTime := time.Now()
		if eligible {
			transitionTime = transitionTime.Add(-repairToleration - time.Second)
		}
		current := env.GetNode(node.Name)
		condition := corev1.NodeCondition{
			Type:               kwokcloudprovider.KWOKUnhealthyCondition,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(transitionTime),
			Reason:             kwokcloudprovider.KWOKUnhealthyReason,
			Message:            "injected repair regression fault",
		}
		env.ReplaceNodeConditions(&current, condition)
		env.ExpectStatusUpdated(&current)
		var persistedTransitionTime metav1.Time
		Eventually(func(g Gomega) {
			persisted := &corev1.Node{}
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(node), persisted)).To(Succeed())
			persistedCondition, found := lo.Find(persisted.Status.Conditions, func(current corev1.NodeCondition) bool {
				return current.Type == kwokcloudprovider.KWOKUnhealthyCondition
			})
			g.Expect(found).To(BeTrue())
			persistedTransitionTime = persistedCondition.LastTransitionTime
		}).Should(Succeed())
		return persistedTransitionTime
	}

	clearFault := func(node *corev1.Node) {
		GinkgoHelper()
		current := env.GetNode(node.Name)
		current.Status.Conditions = lo.Reject(current.Status.Conditions, func(condition corev1.NodeCondition, _ int) bool {
			return condition.Type == kwokcloudprovider.KWOKUnhealthyCondition
		})
		env.ExpectStatusUpdated(&current)
	}

	newWorkload := func(name string, count int, annotations map[string]string, terminationGracePeriodSeconds *int64) (*appsv1.Deployment, labels.Selector) {
		GinkgoHelper()
		appLabels := map[string]string{"app": name}
		deployment := test.Deployment(test.DeploymentOptions{
			Replicas: int32(count),
			PodOptions: test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      appLabels,
					Annotations: annotations,
				},
				PodAntiRequirements:           hostnameAntiAffinity(appLabels),
				TerminationGracePeriodSeconds: terminationGracePeriodSeconds,
			},
		})
		return deployment, labels.SelectorFromSet(appLabels)
	}

	listNodeClaimsForPool := func(g Gomega) []*v1.NodeClaim {
		GinkgoHelper()
		nodeClaims := &v1.NodeClaimList{}
		g.Expect(env.Client.List(env, nodeClaims, client.MatchingLabels{v1.NodePoolLabelKey: nodePool.Name})).To(Succeed())
		return lo.ToSlicePtr(nodeClaims.Items)
	}

	nodeClaimsForPool := func(expected int) []*v1.NodeClaim {
		GinkgoHelper()
		var result []*v1.NodeClaim
		Eventually(func(g Gomega) {
			result = listNodeClaimsForPool(g)
			g.Expect(result).To(HaveLen(expected))
		}).Should(Succeed())
		return result
	}

	nodeClaimForNode := func(node *corev1.Node) *v1.NodeClaim {
		GinkgoHelper()
		var result *v1.NodeClaim
		Eventually(func(g Gomega) {
			matches := lo.Filter(listNodeClaimsForPool(g), func(nodeClaim *v1.NodeClaim, _ int) bool {
				return nodeClaim.Status.ProviderID == node.Spec.ProviderID
			})
			g.Expect(matches).To(HaveLen(1))
			result = matches[0].DeepCopy()
		}).Should(Succeed())
		return result
	}

	nodeClaimUIDs := func(nodeClaims []*v1.NodeClaim) map[types.UID]struct{} {
		return lo.SliceToMap(nodeClaims, func(nodeClaim *v1.NodeClaim) (types.UID, struct{}) {
			return nodeClaim.UID, struct{}{}
		})
	}

	consistentlyExpectTargetUndisrupted := func(target *v1.NodeClaim, node *corev1.Node, originalUIDs map[types.UID]struct{}, duration time.Duration) {
		GinkgoHelper()
		Consistently(func(g Gomega) {
			currentTarget := &v1.NodeClaim{}
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(target), currentTarget)).To(Succeed())
			g.Expect(currentTarget.DeletionTimestamp.IsZero()).To(BeTrue())

			currentNode := &corev1.Node{}
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(node), currentNode)).To(Succeed())
			g.Expect(lo.SomeBy(currentNode.Spec.Taints, func(taint corev1.Taint) bool {
				return taint.MatchTaint(&v1.DisruptedNoScheduleTaint)
			})).To(BeFalse())
			g.Expect(nodeClaimUIDs(listNodeClaimsForPool(g))).To(Equal(originalUIDs))
		}, duration).Should(Succeed())
	}

	eventuallyExpectDeletionStarted := func(target *v1.NodeClaim) {
		GinkgoHelper()
		current := &v1.NodeClaim{}
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(target), current)).To(Succeed())
			g.Expect(current.DeletionTimestamp.IsZero()).To(BeFalse())
		}).Should(Succeed())
	}

	It("should honor the repair toleration and pre-spin a dynamic replacement before deleting the unhealthy NodeClaim", func() {
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "1", Reasons: []v1.DisruptionReason{v1.DisruptionReasonUnhealthy}}}
		deployment, selector := newWorkload("repair-pre-spin", repairNodeCount, nil, nil)
		env.ExpectCreated(nodeClass, nodePool, deployment)
		env.EventuallyExpectHealthyPodCount(selector, repairNodeCount)
		nodes := env.EventuallyExpectNodeCount("==", repairNodeCount)
		originalNodeClaims := nodeClaimsForPool(repairNodeCount)
		originalUIDs := nodeClaimUIDs(originalNodeClaims)

		target := nodeClaimForNode(nodes[0])
		target.Finalizers = append(target.Finalizers, common.TestingFinalizer)
		env.ExpectUpdated(target)
		faultTransitionTime := injectFault(nodes[0], false)
		eligibleAt := faultTransitionTime.Add(repairToleration)
		preEligibilityWindow := time.Until(eligibleAt.Add(-5 * time.Second))
		Expect(preEligibilityWindow).To(BeNumerically(">", 0), "repair toleration window elapsed before assertion")
		consistentlyExpectTargetUndisrupted(target, nodes[0], originalUIDs, preEligibilityWindow)

		var replacement *v1.NodeClaim
		Eventually(func(g Gomega) {
			currentTarget := &v1.NodeClaim{}
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(target), currentTarget)).To(Succeed())
			g.Expect(currentTarget.DeletionTimestamp.IsZero()).To(BeFalse())
			g.Expect(currentTarget.DeletionTimestamp.Time.Before(eligibleAt)).To(BeFalse())

			replacements := lo.Reject(listNodeClaimsForPool(g), func(nodeClaim *v1.NodeClaim, _ int) bool {
				_, ok := originalUIDs[nodeClaim.UID]
				return ok
			})
			g.Expect(replacements).To(HaveLen(1))
			g.Expect(replacements[0].StatusConditions().Root().IsTrue()).To(BeTrue())
			g.Expect(replacements[0].CreationTimestamp.Time.Before(eligibleAt)).To(BeFalse())
			g.Expect(replacements[0].CreationTimestamp.Time.After(currentTarget.DeletionTimestamp.Time)).To(BeFalse())
			replacement = replacements[0].DeepCopy()
		}).Should(Succeed())

		Expect(env.ExpectTestingFinalizerRemoved(target)).To(Succeed())
		env.EventuallyExpectNotFound(target)
		env.EventuallyExpectNodeClaimsReady(replacement)
		env.EventuallyExpectNodeCount("==", repairNodeCount)
		env.EventuallyExpectHealthyPodCount(selector, repairNodeCount)
	})

	It("should honor an Unhealthy budget of zero and resume after the budget is raised", func() {
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0", Reasons: []v1.DisruptionReason{v1.DisruptionReasonUnhealthy}}}
		deployment, selector := newWorkload("repair-budget-zero", repairNodeCount, nil, nil)
		env.ExpectCreated(nodeClass, nodePool, deployment)
		env.EventuallyExpectHealthyPodCount(selector, repairNodeCount)
		nodes := env.EventuallyExpectNodeCount("==", repairNodeCount)
		originalNodeClaims := nodeClaimsForPool(repairNodeCount)
		originalUIDs := nodeClaimUIDs(originalNodeClaims)
		target := nodeClaimForNode(nodes[0])
		target.Finalizers = append(target.Finalizers, common.TestingFinalizer)
		env.ExpectUpdated(target)
		injectFault(nodes[0], true)

		consistentlyExpectTargetUndisrupted(target, nodes[0], originalUIDs, 30*time.Second)

		nodePool.Spec.Disruption.Budgets[0].Nodes = "1"
		env.ExpectUpdated(nodePool)
		eventuallyExpectDeletionStarted(target)
		Expect(env.ExpectTestingFinalizerRemoved(target)).To(Succeed())
		env.EventuallyExpectNotFound(target)
		env.EventuallyExpectNodeCount("==", repairNodeCount)
		env.EventuallyExpectHealthyPodCount(selector, repairNodeCount)
	})

	It("should pace two eligible repairs to one concurrent disruption", func() {
		const nodeCount = 11
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "1", Reasons: []v1.DisruptionReason{v1.DisruptionReasonUnhealthy}}}
		deployment, selector := newWorkload(
			"repair-budget-one",
			nodeCount,
			map[string]string{v1.DoNotDisruptAnnotationKey: "true"},
			lo.ToPtr[int64](0),
		)
		env.ExpectCreated(nodeClass, nodePool, deployment)
		env.EventuallyExpectHealthyPodCount(selector, nodeCount)
		nodes := env.EventuallyExpectNodeCount("==", nodeCount)
		originalUIDs := nodeClaimUIDs(nodeClaimsForPool(nodeCount))
		targets := nodes[:2]
		targetNodeClaims := []*v1.NodeClaim{nodeClaimForNode(targets[0]), nodeClaimForNode(targets[1])}
		for _, target := range targetNodeClaims {
			target.Finalizers = append(target.Finalizers, common.TestingFinalizer)
			env.ExpectUpdated(target)
		}

		injectFault(targets[0], true)
		injectFault(targets[1], true)
		var activeTarget, pendingTarget *v1.NodeClaim
		var pendingNode *corev1.Node
		Eventually(func(g Gomega) {
			currentTargets := make([]*v1.NodeClaim, 0, len(targetNodeClaims))
			for _, target := range targetNodeClaims {
				current := &v1.NodeClaim{}
				g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(target), current)).To(Succeed())
				currentTargets = append(currentTargets, current)
			}
			active := lo.Filter(currentTargets, func(target *v1.NodeClaim, _ int) bool {
				return !target.DeletionTimestamp.IsZero()
			})
			pending := lo.Filter(currentTargets, func(target *v1.NodeClaim, _ int) bool {
				return target.DeletionTimestamp.IsZero()
			})
			g.Expect(active).To(HaveLen(1))
			g.Expect(pending).To(HaveLen(1))
			activeTarget = active[0].DeepCopy()
			pendingTarget = pending[0].DeepCopy()
			pendingNode = lo.Ternary(
				pendingTarget.UID == targetNodeClaims[0].UID,
				targets[0].DeepCopy(),
				targets[1].DeepCopy(),
			)

			replacements := lo.Reject(listNodeClaimsForPool(g), func(nodeClaim *v1.NodeClaim, _ int) bool {
				_, ok := originalUIDs[nodeClaim.UID]
				return ok
			})
			g.Expect(replacements).To(HaveLen(1))
			g.Expect(replacements[0].StatusConditions().Root().IsTrue()).To(BeTrue())
		}).Should(Succeed())

		Consistently(func(g Gomega) {
			active := &v1.NodeClaim{}
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(activeTarget), active)).To(Succeed())
			g.Expect(active.DeletionTimestamp.IsZero()).To(BeFalse())
			pending := &v1.NodeClaim{}
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(pendingTarget), pending)).To(Succeed())
			g.Expect(pending.DeletionTimestamp.IsZero()).To(BeTrue())
			currentPendingNode := &corev1.Node{}
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(pendingNode), currentPendingNode)).To(Succeed())
			g.Expect(lo.SomeBy(currentPendingNode.Spec.Taints, func(taint corev1.Taint) bool {
				return taint.MatchTaint(&v1.DisruptedNoScheduleTaint)
			})).To(BeFalse())
			replacements := lo.Reject(listNodeClaimsForPool(g), func(nodeClaim *v1.NodeClaim, _ int) bool {
				_, ok := originalUIDs[nodeClaim.UID]
				return ok
			})
			g.Expect(replacements).To(HaveLen(1))
		}, 10*time.Second).Should(Succeed())

		Expect(env.ExpectTestingFinalizerRemoved(activeTarget)).To(Succeed())
		env.EventuallyExpectNotFound(activeTarget)
		Eventually(func(g Gomega) {
			current := &v1.NodeClaim{}
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(pendingTarget), current)).To(Succeed())
			g.Expect(current.DeletionTimestamp.IsZero()).To(BeFalse())
			replacements := lo.Reject(listNodeClaimsForPool(g), func(nodeClaim *v1.NodeClaim, _ int) bool {
				_, ok := originalUIDs[nodeClaim.UID]
				return ok
			})
			g.Expect(replacements).To(HaveLen(2))
			for _, replacement := range replacements {
				g.Expect(replacement.StatusConditions().Root().IsTrue()).To(BeTrue())
			}
		}).Should(Succeed())

		Expect(env.ExpectTestingFinalizerRemoved(pendingTarget)).To(Succeed())
		env.EventuallyExpectNotFound(pendingTarget)
		env.EventuallyExpectHealthyPodCount(selector, nodeCount)
		env.EventuallyExpectNodeCount("==", nodeCount)
	})

	It("should freeze above the unhealthy threshold and resume after enough nodes recover", func() {
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "100%", Reasons: []v1.DisruptionReason{v1.DisruptionReasonUnhealthy}}}
		deployment, selector := newWorkload("repair-breaker", repairNodeCount, nil, nil)
		env.ExpectCreated(nodeClass, nodePool, deployment)
		env.EventuallyExpectHealthyPodCount(selector, repairNodeCount)
		nodes := env.EventuallyExpectNodeCount("==", repairNodeCount)
		targetNodeClaims := []*v1.NodeClaim{
			nodeClaimForNode(nodes[0]),
			nodeClaimForNode(nodes[1]),
			nodeClaimForNode(nodes[2]),
		}
		for _, target := range targetNodeClaims {
			target.Finalizers = append(target.Finalizers, common.TestingFinalizer)
			env.ExpectUpdated(target)
		}

		injectFault(nodes[0], true)
		injectFault(nodes[1], true)
		injectFault(nodes[2], true)
		env.ConsistentlyExpectNoDisruptions(repairNodeCount, 30*time.Second)

		clearFault(nodes[0])
		clearFault(nodes[1])
		eventuallyExpectDeletionStarted(targetNodeClaims[2])
		for _, recovered := range targetNodeClaims[:2] {
			current := &v1.NodeClaim{}
			Expect(env.Client.Get(env, client.ObjectKeyFromObject(recovered), current)).To(Succeed())
			Expect(current.DeletionTimestamp.IsZero()).To(BeTrue())
			Expect(env.ExpectTestingFinalizerRemoved(recovered)).To(Succeed())
		}
		Expect(env.ExpectTestingFinalizerRemoved(targetNodeClaims[2])).To(Succeed())
		env.EventuallyExpectNotFound(nodes[2], targetNodeClaims[2])
		env.EventuallyExpectNodeCount("==", repairNodeCount)
		env.EventuallyExpectHealthyPodCount(selector, repairNodeCount)
	})

	It("should honor do-not-repair and proceed after the annotation is removed", func() {
		deployment, selector := newWorkload("repair-veto", repairNodeCount, nil, nil)
		env.ExpectCreated(nodeClass, nodePool, deployment)
		env.EventuallyExpectHealthyPodCount(selector, repairNodeCount)
		nodes := env.EventuallyExpectNodeCount("==", repairNodeCount)
		originalNodeClaims := nodeClaimsForPool(repairNodeCount)
		originalUIDs := nodeClaimUIDs(originalNodeClaims)
		target := nodeClaimForNode(nodes[0])
		target.Finalizers = append(target.Finalizers, common.TestingFinalizer)
		env.ExpectUpdated(target)

		nodes[0].Annotations = lo.Assign(nodes[0].Annotations, map[string]string{v1.DoNotRepairAnnotationKey: "true"})
		env.ExpectUpdated(nodes[0])
		injectFault(nodes[0], true)
		consistentlyExpectTargetUndisrupted(target, nodes[0], originalUIDs, 30*time.Second)

		current := env.GetNode(nodes[0].Name)
		delete(current.Annotations, v1.DoNotRepairAnnotationKey)
		env.ExpectUpdated(&current)
		eventuallyExpectDeletionStarted(target)
		Expect(env.ExpectTestingFinalizerRemoved(target)).To(Succeed())
		env.EventuallyExpectNotFound(nodes[0], target)
		env.EventuallyExpectNodeCount("==", repairNodeCount)
		env.EventuallyExpectHealthyPodCount(selector, repairNodeCount)
	})

	It("should ignore node do-not-disrupt and bound a pod-blocked drain to the repair policy grace period", func() {
		nodePool.Spec.Template.Spec.TerminationGracePeriod = nil
		deployment, selector := newWorkload(
			"repair-drain-bound",
			repairNodeCount,
			map[string]string{v1.DoNotDisruptAnnotationKey: "true"},
			lo.ToPtr[int64](0),
		)
		env.ExpectCreated(nodeClass, nodePool, deployment)
		pods := env.EventuallyExpectHealthyPodCount(selector, repairNodeCount)
		nodes := env.EventuallyExpectNodeCount("==", repairNodeCount)
		targetPod, ok := lo.Find(pods, func(pod *corev1.Pod) bool { return pod.Spec.NodeName == nodes[0].Name })
		Expect(ok).To(BeTrue())
		target := nodeClaimForNode(nodes[0])

		nodes[0].Annotations = lo.Assign(nodes[0].Annotations, map[string]string{v1.DoNotDisruptAnnotationKey: "true"})
		env.ExpectUpdated(nodes[0])
		injectFault(nodes[0], true)

		var deletionStarted time.Time
		Eventually(func(g Gomega) {
			current := &v1.NodeClaim{}
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(target), current)).To(Succeed())
			g.Expect(current.DeletionTimestamp.IsZero()).To(BeFalse())
			deletionStarted = current.DeletionTimestamp.Time
		}).Should(Succeed())

		lowerBoundWindow := time.Until(deletionStarted.Add(repairDrainBound - 10*time.Second))
		Expect(lowerBoundWindow).To(BeNumerically(">", 0), "repair drain lower-bound window elapsed before assertion")
		Consistently(func(g Gomega) {
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(nodes[0]), &corev1.Node{})).To(Succeed())
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(targetPod), &corev1.Pod{})).To(Succeed())
		}, lowerBoundWindow).Should(Succeed())

		upperBoundWindow := time.Until(deletionStarted.Add(2 * repairDrainBound))
		Expect(upperBoundWindow).To(BeNumerically(">", 0), "repair drain upper-bound window elapsed before assertion")
		env.EventuallyExpectNotFoundAssertion(nodes[0], targetPod, target).WithTimeout(upperBoundWindow).Should(Succeed())
		env.EventuallyExpectNodeCount("==", repairNodeCount)
		env.EventuallyExpectHealthyPodCount(selector, repairNodeCount)
	})

	It("should terminate first for a static NodePool at its node limit without exceeding the limit", func() {
		nodePool.Spec.Replicas = lo.ToPtr[int64](repairNodeCount)
		nodePool.Spec.Limits = v1.Limits{corev1.ResourceName("nodes"): resource.MustParse("6")}
		nodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "1", Reasons: []v1.DisruptionReason{v1.DisruptionReasonUnhealthy}}}
		addStaticKWOKRequirements(nodePool)
		deployment, selector := newWorkload("repair-static-at-limit", repairNodeCount, nil, nil)
		env.ExpectCreated(nodeClass, nodePool, deployment)
		env.EventuallyExpectHealthyPodCount(selector, repairNodeCount)
		nodes := env.EventuallyExpectNodeCount("==", repairNodeCount)
		originalNodeClaims := nodeClaimsForPool(repairNodeCount)
		originalUIDs := nodeClaimUIDs(originalNodeClaims)
		target := nodeClaimForNode(nodes[0])
		target.Finalizers = append(target.Finalizers, common.TestingFinalizer)
		env.ExpectUpdated(target)
		injectFault(nodes[0], true)

		Eventually(func(g Gomega) {
			currentTarget := &v1.NodeClaim{}
			g.Expect(env.Client.Get(env, client.ObjectKeyFromObject(target), currentTarget)).To(Succeed())
			g.Expect(currentTarget.DeletionTimestamp.IsZero()).To(BeFalse())
			nodeClaims := listNodeClaimsForPool(g)
			g.Expect(nodeClaims).To(HaveLen(repairNodeCount))
			g.Expect(nodeClaimUIDs(nodeClaims)).To(Equal(originalUIDs))
		}).Should(Succeed())

		Expect(env.ExpectTestingFinalizerRemoved(target)).To(Succeed())
		env.EventuallyExpectNotFound(target)
		Eventually(func(g Gomega) {
			nodeClaims := listNodeClaimsForPool(g)
			g.Expect(nodeClaims).To(HaveLen(repairNodeCount))
			g.Expect(nodeClaimUIDs(nodeClaims)).ToNot(HaveKey(target.UID))
			g.Expect(lo.CountBy(nodeClaims, func(nodeClaim *v1.NodeClaim) bool {
				_, original := originalUIDs[nodeClaim.UID]
				return !original && nodeClaim.StatusConditions().Root().IsTrue()
			})).To(Equal(1))
		}).Should(Succeed())

		env.EventuallyExpectNodeCount("==", repairNodeCount)
		env.EventuallyExpectHealthyPodCount(selector, repairNodeCount)
	})
})

// hostnameAntiAffinity forces each pod carrying the given labels onto its own node.
func hostnameAntiAffinity(matchLabels map[string]string) []corev1.PodAffinityTerm {
	return []corev1.PodAffinityTerm{{
		TopologyKey:   corev1.LabelHostname,
		LabelSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
	}}
}

func addStaticKWOKRequirements(nodePool *v1.NodePool) {
	nodePool.Spec.Template.Spec.Requirements = append(nodePool.Spec.Template.Spec.Requirements, v1.NodeSelectorRequirementWithMinValues{
		Key:      corev1.LabelInstanceTypeStable,
		Operator: corev1.NodeSelectorOpIn,
		Values:   []string{"c-16x-amd64-linux", "c-16x-arm64-linux"},
	})
}
