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

package health

import (
	"context"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/reasonable"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"

	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

var allowedUnhealthyPercent = intstr.FromString("20%")

// Controller for the resource
type Controller struct {
	clock               clock.Clock
	recorder            events.Recorder
	kubeClient          client.Client
	cloudProvider       cloudprovider.CloudProvider
	repairPolicyMatcher *RepairPolicyMatcher
}

// NewController validates the cloud provider's repair policies and constructs a controller instance.
func NewController(kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, clock clock.Clock, recorder events.Recorder) (*Controller, error) {
	repairPolicyMatcher, err := NewRepairPolicyMatcher(cloudProvider.RepairPolicies(), sets.New(cloudprovider.ReplaceNode))
	if err != nil {
		return nil, err
	}
	return &Controller{
		clock:               clock,
		recorder:            recorder,
		kubeClient:          kubeClient,
		cloudProvider:       cloudProvider,
		repairPolicyMatcher: repairPolicyMatcher,
	}, nil
}

func (c *Controller) Name() string {
	return "node.health"
}

func (c *Controller) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&corev1.Node{}, builder.WithPredicates(nodeutils.IsManagedPredicateFuncs(c.cloudProvider), predicate.Funcs{UpdateFunc: c.nodeHealthChanged})).
		WithOptions(controller.Options{
			RateLimiter:             reasonable.RateLimiter(),
			MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), 10, 1000),
		}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

func (c *Controller) nodeHealthChanged(e event.UpdateEvent) bool {
	oldNode := e.ObjectOld.(*corev1.Node)
	newNode := e.ObjectNew.(*corev1.Node)
	if len(oldNode.Status.Conditions) != len(newNode.Status.Conditions) {
		return true
	}

	for _, oldCondition := range oldNode.Status.Conditions {
		newCondition := nodeutils.GetCondition(newNode, oldCondition.Type)
		if newCondition.Type == "" ||
			oldCondition.LastTransitionTime != newCondition.LastTransitionTime ||
			oldCondition.Status != newCondition.Status {
			return true
		}
		if oldCondition.Reason != newCondition.Reason && c.repairPolicyMatcher.hasReasonPolicies(oldCondition) {
			return true
		}
	}
	return false
}

func (c *Controller) Reconcile(ctx context.Context, node *corev1.Node) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	// Validate that the node is owned by us
	nodeClaim, err := nodeutils.NodeClaimForNode(ctx, c.kubeClient, node)
	if err != nil {
		return reconcile.Result{}, nodeutils.IgnoreDuplicateNodeClaimError(nodeutils.IgnoreNodeClaimNotFoundError(err))
	}
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues(
		"Node", klog.KObj(node),
		"NodeClaim", klog.KObj(nodeClaim),
	))

	now := c.clock.Now()
	decision, ok := c.selectRepairDecision(node, now)
	if !ok {
		return reconcile.Result{}, nil
	}
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues(decision.logValues()...))

	// If no matching policy is eligible, requeue when the next policy becomes eligible.
	if !decision.eligible {
		log.FromContext(ctx).V(1).Info("waiting for repair policy eligibility")
		return reconcile.Result{RequeueAfter: decision.eligibleAt.Sub(now)}, nil
	}
	log.FromContext(ctx).V(1).Info("repair policy eligible")

	// If a nodeclaim does have a nodepool label, validate the nodeclaims inside the nodepool are healthy (i.e bellow the allowed threshold)
	// In the case of standalone nodeclaim, validate the nodes inside the cluster are healthy before proceeding
	// to repair the nodes
	nodePoolName, found := nodeClaim.Labels[v1.NodePoolLabelKey]
	if found {
		nodePoolHealthy, err := c.isNodePoolHealthy(ctx, nodePoolName)
		if err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
		if !nodePoolHealthy {
			if err := c.publishNodePoolHealthEvent(ctx, node, nodeClaim, nodePoolName); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{RequeueAfter: 5 * time.Minute}, nil
		}
	} else {
		clusterHealthy, err := c.isClusterHealthy(ctx)
		if err != nil {
			return reconcile.Result{}, err
		}
		if !clusterHealthy {
			c.recorder.Publish(NodeRepairBlockedUnmanagedNodeClaim(node, nodeClaim, fmt.Sprintf("more then %s nodes are unhealthy in the cluster", allowedUnhealthyPercent.String()))...)
			return reconcile.Result{RequeueAfter: 5 * time.Minute}, nil
		}
	}
	// This controller currently supports only ReplaceNode and executes it through forceful NodeClaim deletion.
	if err := c.annotateTerminationGracePeriod(ctx, nodeClaim); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	return c.deleteNodeClaim(ctx, nodeClaim, node, decision)
}

// deleteNodeClaim removes the NodeClaim from the api-server
func (c *Controller) deleteNodeClaim(ctx context.Context, nodeClaim *v1.NodeClaim, node *corev1.Node, decision repairDecision) (reconcile.Result, error) {
	if !nodeClaim.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}
	if err := c.kubeClient.Delete(ctx, nodeClaim); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	// The deletion timestamp has successfully been set for the NodeClaim, update relevant metrics.
	log.FromContext(ctx).Info("deleting unhealthy node")
	labels := map[string]string{
		metrics.ReasonLabel:              metrics.UnhealthyReason,
		metrics.NodePoolLabel:            node.Labels[v1.NodePoolLabelKey],
		metrics.CapacityTypeLabel:        node.Labels[v1.CapacityTypeLabelKey],
		metrics.ConsolidationPolicyLabel: "",
		metrics.TerminationModeLabel:     nodeclaimutils.DisruptionTerminationMode(nodeClaim),
	}
	metrics.NodeClaimsDisruptedTotal.Inc(labels)
	// Pods on the node have not yet started draining at this point — list captures
	// the pre-disruption state. Errors don't fail the reconcile; the metric reports 0.
	reschedulablePods, err := nodeutils.ReschedulablePods(ctx, c.kubeClient, node.Name)
	if err != nil {
		log.FromContext(ctx).V(1).Info("listing reschedulable pods for disruption metric", "error", err.Error())
	}
	metrics.PodsDisruptionInitiatedTotal.Add(float64(len(reschedulablePods)), labels)
	NodeClaimsUnhealthyDisruptedTotal.Inc(map[string]string{
		ConditionLabel:            pretty.ToSnakeCase(string(decision.condition.Type)),
		metrics.NodePoolLabel:     node.Labels[v1.NodePoolLabelKey],
		metrics.CapacityTypeLabel: node.Labels[v1.CapacityTypeLabelKey],
		ImageIDLabel:              nodeClaim.Status.ImageID,
	})
	return reconcile.Result{}, nil
}

// selectRepairDecision prefers eligible decisions, then selects the decision with the earliest eligibility time.
func (c *Controller) selectRepairDecision(node *corev1.Node, now time.Time) (repairDecision, bool) {
	var selected repairDecision
	selectedSet := false
	for _, condition := range node.Status.Conditions {
		decision, ok := c.repairPolicyMatcher.evaluateDecision(condition, now)
		if !ok {
			continue
		}
		if !selectedSet ||
			(decision.eligible && !selected.eligible) ||
			(decision.eligible == selected.eligible && decision.eligibleAt.Before(selected.eligibleAt)) {
			selected = decision
			selectedSet = true
		}
	}
	return selected, selectedSet
}

func (c *Controller) annotateTerminationGracePeriod(ctx context.Context, nodeClaim *v1.NodeClaim) error {
	if expirationTimeString, exists := nodeClaim.Annotations[v1.NodeClaimTerminationTimestampAnnotationKey]; exists {
		expirationTime, err := time.Parse(time.RFC3339, expirationTimeString)
		if err == nil && expirationTime.Before(c.clock.Now()) {
			return nil
		}
	}
	stored := nodeClaim.DeepCopy()
	terminationTime := c.clock.Now().Format(time.RFC3339)
	nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.NodeClaimTerminationTimestampAnnotationKey: terminationTime})

	if !equality.Semantic.DeepEqual(stored, nodeClaim) {
		if err := c.kubeClient.Patch(ctx, nodeClaim, client.MergeFrom(stored)); err != nil {
			return err
		}
		log.FromContext(ctx).WithValues(v1.NodeClaimTerminationTimestampAnnotationKey, terminationTime).Info("annotated nodeclaim")
	}
	return nil
}

// isNodePoolHealthy checks if the number of unhealthy nodes managed by the given NodePool exceeds the health threshold.
// defined by the cloud provider
// Up to 20% of Nodes may be unhealthy before the NodePool becomes unhealthy (or the nearest whole number, rounding up).
// For example, given a NodePool with three nodes, one may be unhealthy without rendering the NodePool unhealthy, even though that's 33% of the total nodes.
// This is analogous to how minAvailable and maxUnavailable work for PodDisruptionBudgets: https://kubernetes.io/docs/tasks/run-application/configure-pdb/#rounding-logic-when-specifying-percentages.
func (c *Controller) isNodePoolHealthy(ctx context.Context, nodePoolName string) (bool, error) {
	return c.areNodesHealthy(ctx, client.MatchingLabels(map[string]string{v1.NodePoolLabelKey: nodePoolName}))
}

func (c *Controller) isClusterHealthy(ctx context.Context) (bool, error) {
	return c.areNodesHealthy(ctx)
}

func (c *Controller) areNodesHealthy(ctx context.Context, opts ...client.ListOption) (bool, error) {
	nodeList := &corev1.NodeList{}
	if err := c.kubeClient.List(ctx, nodeList, append(opts, client.UnsafeDisableDeepCopy)...); err != nil {
		return false, err
	}
	unhealthyNodeCount := lo.CountBy(nodeList.Items, func(node corev1.Node) bool {
		return lo.SomeBy(node.Status.Conditions, func(condition corev1.NodeCondition) bool {
			return c.repairPolicyMatcher.hasCondition(condition)
		})
	})
	threshold := lo.Must(intstr.GetScaledValueFromIntOrPercent(new(allowedUnhealthyPercent), len(nodeList.Items), true))
	return unhealthyNodeCount <= threshold, nil
}

func (c *Controller) publishNodePoolHealthEvent(ctx context.Context, node *corev1.Node, nodeClaim *v1.NodeClaim, npName string) error {
	np := &v1.NodePool{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: npName}, np); err != nil {
		return client.IgnoreNotFound(err)
	}
	c.recorder.Publish(NodeRepairBlocked(node, nodeClaim, np, fmt.Sprintf("more than %s nodes are unhealthy in the nodepool", allowedUnhealthyPercent.String()))...)
	return nil
}
