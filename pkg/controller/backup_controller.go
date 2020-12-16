/*
Copyright 2020 the Velero contributors.

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

package controller

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	kerrors "k8s.io/apimachinery/pkg/util/errors"

	snapshotv1beta1api "github.com/kubernetes-csi/external-snapshotter/v2/pkg/apis/volumesnapshot/v1beta1"
	snapshotv1beta1listers "github.com/kubernetes-csi/external-snapshotter/v2/pkg/client/listers/volumesnapshot/v1beta1"

	"github.com/vmware-tanzu/velero/internal/storage"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	pkgbackup "github.com/vmware-tanzu/velero/pkg/backup"
	"github.com/vmware-tanzu/velero/pkg/discovery"
	"github.com/vmware-tanzu/velero/pkg/features"
	velerov1client "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned/typed/velero/v1"
	velerov1listers "github.com/vmware-tanzu/velero/pkg/generated/listers/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	"github.com/vmware-tanzu/velero/pkg/persistence"
	"github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/collections"
	"github.com/vmware-tanzu/velero/pkg/util/encode"
	kubeutil "github.com/vmware-tanzu/velero/pkg/util/kube"
	"github.com/vmware-tanzu/velero/pkg/util/logging"
	"github.com/vmware-tanzu/velero/pkg/volume"

	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// BackupReconciler reconciles backup objects
type BackupReconciler struct {
	client                      kbclient.Client
	backupper                   pkgbackup.Backupper
	clock                       clock.Clock
	newPluginManager            func(logrus.FieldLogger) clientmgmt.Manager
	metrics                     *metrics.ServerMetrics
	newBackupStore              func(*velerov1api.BackupStorageLocation, persistence.ObjectStoreGetter, logrus.FieldLogger) (persistence.BackupStore, error)
	formatFlag                  logging.Format
	backupLogLevel              logrus.Level
	Log                         logrus.FieldLogger
	volumeSnapshotLister        snapshotv1beta1listers.VolumeSnapshotLister
	volumeSnapshotContentLister snapshotv1beta1listers.VolumeSnapshotContentLister
	snapshotLocationLister      velerov1listers.VolumeSnapshotLocationLister
	defaultSnapshotLocations    map[string]string
	defaultBackupTTL            time.Duration
	defaultBackupLocation       string
	defaultVolumesToRestic      bool
	discoveryHelper             discovery.Helper
	backupTracker               BackupTracker
}

type backupController struct {
	*genericController
	discoveryHelper             discovery.Helper
	backupper                   pkgbackup.Backupper
	lister                      velerov1listers.BackupLister
	client                      velerov1client.BackupsGetter
	kbClient                    kbclient.Client
	clock                       clock.Clock
	backupLogLevel              logrus.Level
	newPluginManager            func(logrus.FieldLogger) clientmgmt.Manager
	backupTracker               BackupTracker
	defaultBackupLocation       string
	defaultVolumesToRestic      bool
	defaultBackupTTL            time.Duration
	snapshotLocationLister      velerov1listers.VolumeSnapshotLocationLister
	defaultSnapshotLocations    map[string]string
	metrics                     *metrics.ServerMetrics
	newBackupStore              func(*velerov1api.BackupStorageLocation, persistence.ObjectStoreGetter, logrus.FieldLogger) (persistence.BackupStore, error)
	formatFlag                  logging.Format
	volumeSnapshotLister        snapshotv1beta1listers.VolumeSnapshotLister
	volumeSnapshotContentLister snapshotv1beta1listers.VolumeSnapshotContentLister
}

// NewBackupReconciler returns a new backup reconciler
func NewBackupReconciler(
	client kbclient.Client,
	discoveryHelper discovery.Helper,
	backupper pkgbackup.Backupper,
	logger logrus.FieldLogger,
	backupLogLevel logrus.Level,
	newPluginManager func(logrus.FieldLogger) clientmgmt.Manager,
	backupTracker BackupTracker,
	defaultBackupLocation string,
	defaultVolumesToRestic bool,
	defaultBackupTTL time.Duration,
	volumeSnapshotLocationLister velerov1listers.VolumeSnapshotLocationLister,
	defaultSnapshotLocations map[string]string,
	metrics *metrics.ServerMetrics,
	formatFlag logging.Format,
	volumeSnapshotLister snapshotv1beta1listers.VolumeSnapshotLister,
	volumeSnapshotContentLister snapshotv1beta1listers.VolumeSnapshotContentLister,
) *BackupReconciler {
	return &BackupReconciler{
		client:                      client,
		discoveryHelper:             discoveryHelper,
		backupper:                   backupper,
		Log:                         logger,
		backupLogLevel:              backupLogLevel,
		newPluginManager:            newPluginManager,
		backupTracker:               backupTracker,
		defaultBackupLocation:       defaultBackupLocation,
		defaultVolumesToRestic:      defaultVolumesToRestic,
		defaultBackupTTL:            defaultBackupTTL,
		snapshotLocationLister:      volumeSnapshotLocationLister,
		defaultSnapshotLocations:    defaultSnapshotLocations,
		metrics:                     metrics,
		formatFlag:                  formatFlag,
		volumeSnapshotLister:        volumeSnapshotLister,
		volumeSnapshotContentLister: volumeSnapshotContentLister,
		newBackupStore:              persistence.NewObjectBackupStore,
		clock:                       &clock.RealClock{},
	}
}

// SetupWithManager sets up BackupReconciler with a controller manager.
func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(options).
		For(&velerov1api.Backup{}).
		WithEventFilter(
			predicate.Funcs{
				// Backup objects are patched with status updates as the backup is processed. These patches also trigger reconciliation.
				// We don't want the Reconcile method to be invoked from status updates. So filter these events for Backup resources.
				UpdateFunc: func(e event.UpdateEvent) bool {
					if e.ObjectOld.GetObjectKind().GroupVersionKind().Kind != "Backup" {
						return true
					}

					oldBackup := e.ObjectOld.(*velerov1api.Backup).DeepCopy()
					newBackup := e.ObjectNew.(*velerov1api.Backup).DeepCopy()

					oldBackup.Status = velerov1api.BackupStatus{}
					newBackup.Status = velerov1api.BackupStatus{}

					oldBackup.ObjectMeta.ResourceVersion = ""
					newBackup.ObjectMeta.ResourceVersion = ""

					return !reflect.DeepEqual(oldBackup, newBackup)
				},
			},
		).
		Complete(r)
}

// +kubebuilder:rbac:groups=velero.io,resources=backup,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velero.io,resources=backup/status,verbs=get;update;patch
func (r *BackupReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	r.Log = r.Log.WithField("controller", Backup)
	ctx := context.TODO()
	log := r.Log.WithField("backup", req.NamespacedName)

	log.Info("Reconciling backup")
	backup := &velerov1api.Backup{}
	err := r.client.Get(ctx, req.NamespacedName, backup)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	patchHelper, err := patch.NewHelper(backup, r.client)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "Failed to initialize patch helper")
	}

	// Double-check we have the correct phase. In the unlikely event that multiple controller
	// instances are running, it's possible for controller A to succeed in changing the phase to
	// InProgress, while controller B's attempt to patch the phase fails. When controller B
	// reprocesses the same backup, it will either show up as New (informer hasn't seen the update
	// yet) or as InProgress. In the former case, the patch attempt will fail again, until the
	// informer sees the update. In the latter case, after the informer has seen the update to
	// InProgress, we still need this check so we can return nil to indicate we've finished processing
	// this key (even though it was a no-op).
	switch backup.Status.Phase {
	case "", velerov1api.BackupPhaseNew:
		// only process new backups
	default:
		log.WithField("phase", backup.Status.Phase).Info("Backup is not New, skipping reconciliation")
		return ctrl.Result{}, nil
	}

	log.Debug("Preparing backup request")
	backupReq := r.prepareBackupRequest(backup)

	if len(backupReq.Status.ValidationErrors) > 0 {
		backupReq.Status.Phase = velerov1api.BackupPhaseFailedValidation
		backup.Status.Phase = velerov1api.BackupPhaseFailedValidation
	} else {
		backupReq.Status.Phase = velerov1api.BackupPhaseInProgress
		backup.Status.Phase = velerov1api.BackupPhaseInProgress
		backupReq.Status.StartTimestamp = &metav1.Time{Time: r.clock.Now()}
		backup.Status.StartTimestamp = backupReq.Status.StartTimestamp
	}

	if err := patchHelper.Patch(ctx, backup); err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "Failed to patch backup status to %s", backup.Status.Phase)
	}

	if backupReq.Status.Phase == velerov1api.BackupPhaseFailedValidation {
		return ctrl.Result{}, errors.Wrapf(err, "Backup failed validation")
	}
	r.backupTracker.Add(req.Namespace, req.Name)
	defer r.backupTracker.Delete(req.Namespace, req.Name)

	log.Debug("Running backup")

	backupScheduleName := backupReq.GetLabels()[velerov1api.ScheduleNameLabel]
	r.metrics.RegisterBackupAttempt(backupScheduleName)

	// execution & upload of backup
	if err := r.runBackup(backupReq); err != nil {
		// even though runBackup sets the backup's phase prior
		// to uploading artifacts to object storage, we have to
		// check for an error again here and update the phase if
		// one is found, because there could've been an error
		// while uploading artifacts to object storage, which would
		// result in the backup being Failed.
		log.WithError(err).Error("backup failed")
		backupReq.Status.Phase = velerov1api.BackupPhaseFailed
	}

	switch backupReq.Status.Phase {
	case velerov1api.BackupPhaseCompleted:
		r.metrics.RegisterBackupSuccess(backupScheduleName)
	case velerov1api.BackupPhasePartiallyFailed:
		r.metrics.RegisterBackupPartialFailure(backupScheduleName)
	case velerov1api.BackupPhaseFailed:
		r.metrics.RegisterBackupFailed(backupScheduleName)
	case velerov1api.BackupPhaseFailedValidation:
		r.metrics.RegisterBackupValidationFailure(backupScheduleName)
	}
	log.Debug("Updating backup's final status")
	backup = backupReq.Backup.DeepCopy()
	if err := patchHelper.Patch(ctx, backup); err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "Failed to patch backup's final status to %s", backup.Status.Phase)
	}

	return ctrl.Result{}, nil
}

// getLastSuccessBySchedule finds the most recent completed backup for each schedule
// and returns a map of schedule name -> completion time of the most recent completed
// backup. This map includes an entry for ad-hoc/non-scheduled backups, where the key
// is the empty string.
func getLastSuccessBySchedule(backups []*velerov1api.Backup) map[string]time.Time {
	lastSuccessBySchedule := map[string]time.Time{}
	for _, backup := range backups {
		if backup.Status.Phase != velerov1api.BackupPhaseCompleted {
			continue
		}
		if backup.Status.CompletionTimestamp == nil {
			continue
		}

		schedule := backup.Labels[velerov1api.ScheduleNameLabel]
		timestamp := backup.Status.CompletionTimestamp.Time

		if timestamp.After(lastSuccessBySchedule[schedule]) {
			lastSuccessBySchedule[schedule] = timestamp
		}
	}

	return lastSuccessBySchedule
}

func patchBackup(original, updated *velerov1api.Backup, client velerov1client.BackupsGetter) (*velerov1api.Backup, error) {
	origBytes, err := json.Marshal(original)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling original backup")
	}

	updatedBytes, err := json.Marshal(updated)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling updated backup")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return nil, errors.Wrap(err, "error creating json merge patch for backup")
	}

	res, err := client.Backups(original.Namespace).Patch(context.TODO(), original.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error patching backup")
	}

	return res, nil
}

func (r *BackupReconciler) prepareBackupRequest(backup *velerov1api.Backup) *pkgbackup.Request {
	request := &pkgbackup.Request{
		Backup: backup.DeepCopy(), // don't modify items in the cache
	}

	// set backup major version - deprecated, use Status.FormatVersion
	request.Status.Version = pkgbackup.BackupVersion

	// set backup major, minor, and patch version
	request.Status.FormatVersion = pkgbackup.BackupFormatVersion

	if request.Spec.TTL.Duration == 0 {
		// set default backup TTL
		request.Spec.TTL.Duration = r.defaultBackupTTL
	}

	// calculate expiration
	request.Status.Expiration = &metav1.Time{Time: r.clock.Now().Add(request.Spec.TTL.Duration)}

	// default storage location if not specified
	if request.Spec.StorageLocation == "" {
		request.Spec.StorageLocation = r.defaultBackupLocation

		locationList, err := storage.ListBackupStorageLocations(context.Background(), r.client, request.Namespace)
		if err == nil {
			for _, location := range locationList.Items {
				if location.Spec.Default {
					request.Spec.StorageLocation = location.Name
					break
				}
			}
		}
	}

	if request.Spec.DefaultVolumesToRestic == nil {
		request.Spec.DefaultVolumesToRestic = &r.defaultVolumesToRestic
	}

	// add the storage location as a label for easy filtering later.
	if request.Labels == nil {
		request.Labels = make(map[string]string)
	}
	request.Labels[velerov1api.StorageLocationLabel] = label.GetValidName(request.Spec.StorageLocation)

	// Getting all information of cluster version - useful for future skip-level migration
	if request.Annotations == nil {
		request.Annotations = make(map[string]string)
	}
	request.Annotations[velerov1api.SourceClusterK8sGitVersionAnnotation] = r.discoveryHelper.ServerVersion().String()
	request.Annotations[velerov1api.SourceClusterK8sMajorVersionAnnotation] = r.discoveryHelper.ServerVersion().Major
	request.Annotations[velerov1api.SourceClusterK8sMinorVersionAnnotation] = r.discoveryHelper.ServerVersion().Minor

	// validate the included/excluded resources
	for _, err := range collections.ValidateIncludesExcludes(request.Spec.IncludedResources, request.Spec.ExcludedResources) {
		request.Status.ValidationErrors = append(request.Status.ValidationErrors, fmt.Sprintf("Invalid included/excluded resource lists: %v", err))
	}

	// validate the included/excluded namespaces
	for _, err := range collections.ValidateIncludesExcludes(request.Spec.IncludedNamespaces, request.Spec.ExcludedNamespaces) {
		request.Status.ValidationErrors = append(request.Status.ValidationErrors, fmt.Sprintf("Invalid included/excluded namespace lists: %v", err))
	}

	// validate the storage location, and store the BackupStorageLocation API obj on the request
	storageLocation := &velerov1api.BackupStorageLocation{}
	if err := r.client.Get(context.Background(), kbclient.ObjectKey{
		Namespace: request.Namespace,
		Name:      request.Spec.StorageLocation,
	}, storageLocation); err != nil {
		if apierrors.IsNotFound(err) {
			request.Status.ValidationErrors = append(request.Status.ValidationErrors, fmt.Sprintf("a BackupStorageLocation CRD with the name specified in the backup spec needs to be created before this backup can be executed. Error: %v", err))
		} else {
			request.Status.ValidationErrors = append(request.Status.ValidationErrors, fmt.Sprintf("error getting backup storage location: %v", err))
		}
	} else {
		request.StorageLocation = storageLocation

		if request.StorageLocation.Spec.AccessMode == velerov1api.BackupStorageLocationAccessModeReadOnly {
			request.Status.ValidationErrors = append(request.Status.ValidationErrors,
				fmt.Sprintf("backup can't be created because backup storage location %s is currently in read-only mode", request.StorageLocation.Name))
		}
	}
	// validate and get the backup's VolumeSnapshotLocations, and store the
	// VolumeSnapshotLocation API objs on the request
	if locs, errs := r.validateAndGetSnapshotLocations(request.Backup); len(errs) > 0 {
		request.Status.ValidationErrors = append(request.Status.ValidationErrors, errs...)
	} else {
		request.Spec.VolumeSnapshotLocations = nil
		for _, loc := range locs {
			request.Spec.VolumeSnapshotLocations = append(request.Spec.VolumeSnapshotLocations, loc.Name)
			request.SnapshotLocations = append(request.SnapshotLocations, loc)
		}
	}

	return request
}

// validateAndGetSnapshotLocations gets a collection of VolumeSnapshotLocation objects that
// this backup will use (returned as a map of provider name -> VSL), and ensures:
// - each location name in .spec.volumeSnapshotLocations exists as a location
// - exactly 1 location per provider
// - a given provider's default location name is added to .spec.volumeSnapshotLocations if one
//   is not explicitly specified for the provider (if there's only one location for the provider,
//   it will automatically be used)
// if backup has snapshotVolume disabled then it returns empty VSL
func (r *BackupReconciler) validateAndGetSnapshotLocations(backup *velerov1api.Backup) (map[string]*velerov1api.VolumeSnapshotLocation, []string) {
	errors := []string{}
	providerLocations := make(map[string]*velerov1api.VolumeSnapshotLocation)

	// if snapshotVolume is set to false then we don't need to validate volumesnapshotlocation
	if boolptr.IsSetToFalse(backup.Spec.SnapshotVolumes) {
		return nil, nil
	}

	for _, locationName := range backup.Spec.VolumeSnapshotLocations {
		// validate each locationName exists as a VolumeSnapshotLocation
		location, err := r.snapshotLocationLister.VolumeSnapshotLocations(backup.Namespace).Get(locationName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				errors = append(errors, fmt.Sprintf("a VolumeSnapshotLocation CRD for the location %s with the name specified in the backup spec needs to be created before this snapshot can be executed. Error: %v", locationName, err))
			} else {
				errors = append(errors, fmt.Sprintf("error getting volume snapshot location named %s: %v", locationName, err))
			}
			continue
		}

		// ensure we end up with exactly 1 location *per provider*
		if providerLocation, ok := providerLocations[location.Spec.Provider]; ok {
			// if > 1 location name per provider as in ["aws-us-east-1" | "aws-us-west-1"] (same provider, multiple names)
			if providerLocation.Name != locationName {
				errors = append(errors, fmt.Sprintf("more than one VolumeSnapshotLocation name specified for provider %s: %s; unexpected name was %s", location.Spec.Provider, locationName, providerLocation.Name))
				continue
			}
		} else {
			// keep track of all valid existing locations, per provider
			providerLocations[location.Spec.Provider] = location
		}
	}

	if len(errors) > 0 {
		return nil, errors
	}

	allLocations, err := r.snapshotLocationLister.VolumeSnapshotLocations(backup.Namespace).List(labels.Everything())
	if err != nil {
		errors = append(errors, fmt.Sprintf("error listing volume snapshot locations: %v", err))
		return nil, errors
	}

	// build a map of provider->list of all locations for the provider
	allProviderLocations := make(map[string][]*velerov1api.VolumeSnapshotLocation)
	for i := range allLocations {
		loc := allLocations[i]
		allProviderLocations[loc.Spec.Provider] = append(allProviderLocations[loc.Spec.Provider], loc)
	}

	// go through each provider and make sure we have/can get a VSL
	// for it
	for provider, locations := range allProviderLocations {
		if _, ok := providerLocations[provider]; ok {
			// backup's spec had a location named for this provider
			continue
		}

		if len(locations) > 1 {
			// more than one possible location for the provider: check
			// the defaults
			defaultLocation := r.defaultSnapshotLocations[provider]
			if defaultLocation == "" {
				errors = append(errors, fmt.Sprintf("provider %s has more than one possible volume snapshot location, and none were specified explicitly or as a default", provider))
				continue
			}
			location, err := r.snapshotLocationLister.VolumeSnapshotLocations(backup.Namespace).Get(defaultLocation)
			if err != nil {
				errors = append(errors, fmt.Sprintf("error getting volume snapshot location named %s: %v", defaultLocation, err))
				continue
			}

			providerLocations[provider] = location
			continue
		}

		// exactly one location for the provider: use it
		providerLocations[provider] = locations[0]
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return providerLocations, nil
}

// runBackup runs and uploads a validated backup. Any error returned from this function
// causes the backup to be Failed; if no error is returned, the backup's status's Errors
// field is checked to see if the backup was a partial failure.
func (r *BackupReconciler) runBackup(backup *pkgbackup.Request) error {
	r.Log.WithField(Backup, kubeutil.NamespaceAndName(backup)).Info("Setting up backup log")

	logFile, err := ioutil.TempFile("", "")
	if err != nil {
		return errors.Wrap(err, "error creating temp file for backup log")
	}
	gzippedLogFile := gzip.NewWriter(logFile)
	// Assuming we successfully uploaded the log file, this will have already been closed below. It is safe to call
	// close multiple times. If we get an error closing this, there's not really anything we can do about it.
	defer gzippedLogFile.Close()
	defer closeAndRemoveFile(logFile, r.Log.WithField(Backup, kubeutil.NamespaceAndName(backup)))

	// Log the backup to both a backup log file and to stdout. This will help see what happened if the upload of the
	// backup log failed for whatever reason.
	logger := logging.DefaultLogger(r.backupLogLevel, r.formatFlag)
	logger.Out = io.MultiWriter(os.Stdout, gzippedLogFile)

	logCounter := logging.NewLogCounterHook()
	logger.Hooks.Add(logCounter)

	backupLog := logger.WithField(Backup, kubeutil.NamespaceAndName(backup))

	backupLog.Info("Setting up backup temp file")
	backupFile, err := ioutil.TempFile("", "")
	if err != nil {
		return errors.Wrap(err, "error creating temp file for backup")
	}
	defer closeAndRemoveFile(backupFile, backupLog)

	backupLog.Info("Setting up plugin manager")
	pluginManager := r.newPluginManager(backupLog)
	defer pluginManager.CleanupClients()

	backupLog.Info("Getting backup item actions")
	actions, err := pluginManager.GetBackupItemActions()
	if err != nil {
		return err
	}

	backupLog.Info("Setting up backup store to check for backup existence")
	backupStore, err := r.newBackupStore(backup.StorageLocation, pluginManager, backupLog)
	if err != nil {
		return err
	}

	exists, err := backupStore.BackupExists(backup.StorageLocation.Spec.StorageType.ObjectStorage.Bucket, backup.Name)
	if exists || err != nil {
		backup.Status.Phase = velerov1api.BackupPhaseFailed
		backup.Status.CompletionTimestamp = &metav1.Time{Time: r.clock.Now()}
		if err != nil {
			return errors.Wrapf(err, "error checking if backup already exists in object storage")
		}
		return errors.Errorf("backup already exists in object storage")
	}

	var fatalErrs []error
	if err := r.backupper.Backup(backupLog, backup, backupFile, actions, pluginManager); err != nil {
		fatalErrs = append(fatalErrs, err)
	}

	// Empty slices here so that they can be passed in to the persistBackup call later, regardless of whether or not CSI's enabled.
	// This way, we only make the Lister call if the feature flag's on.
	var volumeSnapshots []*snapshotv1beta1api.VolumeSnapshot
	var volumeSnapshotContents []*snapshotv1beta1api.VolumeSnapshotContent
	if features.IsEnabled(velerov1api.CSIFeatureFlag) {
		selector := label.NewSelectorForBackup(backup.Name)

		// Listers are wrapped in a nil check out of caution, since they may not be populated based on the
		// EnableCSI feature flag. This is more to guard against programmer error, as they shouldn't be nil
		// when EnableCSI is on.
		if r.volumeSnapshotLister != nil {
			volumeSnapshots, err = r.volumeSnapshotLister.List(selector)
			if err != nil {
				backupLog.Error(err)
			}
		}

		if r.volumeSnapshotContentLister != nil {
			volumeSnapshotContents, err = r.volumeSnapshotContentLister.List(selector)
			if err != nil {
				backupLog.Error(err)
			}
		}
	}

	// Mark completion timestamp before serializing and uploading.
	// Otherwise, the JSON file in object storage has a CompletionTimestamp of 'null'.
	backup.Status.CompletionTimestamp = &metav1.Time{Time: r.clock.Now()}

	backup.Status.VolumeSnapshotsAttempted = len(backup.VolumeSnapshots)
	for _, snap := range backup.VolumeSnapshots {
		if snap.Status.Phase == volume.SnapshotPhaseCompleted {
			backup.Status.VolumeSnapshotsCompleted++
		}
	}

	recordBackupMetrics(backupLog, backup.Backup, backupFile, r.metrics)

	if err := gzippedLogFile.Close(); err != nil {
		r.Log.WithField(Backup, kubeutil.NamespaceAndName(backup)).WithError(err).Error("error closing gzippedLogFile")
	}

	backup.Status.Warnings = logCounter.GetCount(logrus.WarnLevel)
	backup.Status.Errors = logCounter.GetCount(logrus.ErrorLevel)

	// Assign finalize phase as close to end as possible so that any errors
	// logged to backupLog are captured. This is done before uploading the
	// artifacts to object storage so that the JSON representation of the
	// backup in object storage has the terminal phase set.
	switch {
	case len(fatalErrs) > 0:
		backup.Status.Phase = velerov1api.BackupPhaseFailed
	case logCounter.GetCount(logrus.ErrorLevel) > 0:
		backup.Status.Phase = velerov1api.BackupPhasePartiallyFailed
	default:
		backup.Status.Phase = velerov1api.BackupPhaseCompleted
	}

	// re-instantiate the backup store because credentials could have changed since the original
	// instantiation, if this was a long-running backup
	backupLog.Info("Setting up backup store to persist the backup")
	backupStore, err = r.newBackupStore(backup.StorageLocation, pluginManager, backupLog)
	if err != nil {
		return err
	}

	if errs := persistBackup(backup, backupFile, logFile, backupStore, r.Log.WithField(Backup, kubeutil.NamespaceAndName(backup)), volumeSnapshots, volumeSnapshotContents); len(errs) > 0 {
		fatalErrs = append(fatalErrs, errs...)
	}

	r.Log.WithField(Backup, kubeutil.NamespaceAndName(backup)).Info("Backup completed")

	// if we return a non-nil error, the calling function will update
	// the backup's phase to Failed.
	return kerrors.NewAggregate(fatalErrs)
}

func recordBackupMetrics(log logrus.FieldLogger, backup *velerov1api.Backup, backupFile *os.File, serverMetrics *metrics.ServerMetrics) {
	backupScheduleName := backup.GetLabels()[velerov1api.ScheduleNameLabel]

	var backupSizeBytes int64
	if backupFileStat, err := backupFile.Stat(); err != nil {
		log.WithError(errors.WithStack(err)).Error("Error getting backup file info")
	} else {
		backupSizeBytes = backupFileStat.Size()
	}
	serverMetrics.SetBackupTarballSizeBytesGauge(backupScheduleName, backupSizeBytes)

	backupDuration := backup.Status.CompletionTimestamp.Time.Sub(backup.Status.StartTimestamp.Time)
	backupDurationSeconds := float64(backupDuration / time.Second)
	serverMetrics.RegisterBackupDuration(backupScheduleName, backupDurationSeconds)
	serverMetrics.RegisterVolumeSnapshotAttempts(backupScheduleName, backup.Status.VolumeSnapshotsAttempted)
	serverMetrics.RegisterVolumeSnapshotSuccesses(backupScheduleName, backup.Status.VolumeSnapshotsCompleted)
	serverMetrics.RegisterVolumeSnapshotFailures(backupScheduleName, backup.Status.VolumeSnapshotsAttempted-backup.Status.VolumeSnapshotsCompleted)
}

func persistBackup(backup *pkgbackup.Request,
	backupContents, backupLog *os.File,
	backupStore persistence.BackupStore,
	log logrus.FieldLogger,
	csiVolumeSnapshots []*snapshotv1beta1api.VolumeSnapshot,
	csiVolumeSnapshotContents []*snapshotv1beta1api.VolumeSnapshotContent,
) []error {
	persistErrs := []error{}
	backupJSON := new(bytes.Buffer)

	if err := encode.EncodeTo(backup.Backup, "json", backupJSON); err != nil {
		persistErrs = append(persistErrs, errors.Wrap(err, "error encoding backup"))
	}

	// Velero-native volume snapshots (as opposed to CSI ones)
	nativeVolumeSnapshots, errs := encodeToJSONGzip(backup.VolumeSnapshots, "native volumesnapshots list")
	if errs != nil {
		persistErrs = append(persistErrs, errs...)
	}

	podVolumeBackups, errs := encodeToJSONGzip(backup.PodVolumeBackups, "pod volume backups list")
	if errs != nil {
		persistErrs = append(persistErrs, errs...)
	}

	csiSnapshotJSON, errs := encodeToJSONGzip(csiVolumeSnapshots, "csi volume snapshots list")
	if errs != nil {
		persistErrs = append(persistErrs, errs...)
	}

	csiSnapshotContentsJSON, errs := encodeToJSONGzip(csiVolumeSnapshotContents, "csi volume snapshot contents list")
	if errs != nil {
		persistErrs = append(persistErrs, errs...)
	}

	backupResourceList, errs := encodeToJSONGzip(backup.BackupResourceList(), "backup resources list")
	if errs != nil {
		persistErrs = append(persistErrs, errs...)
	}

	if len(persistErrs) > 0 {
		// Don't upload the JSON files or backup tarball if encoding to json fails.
		backupJSON = nil
		backupContents = nil
		nativeVolumeSnapshots = nil
		backupResourceList = nil
		csiSnapshotJSON = nil
		csiSnapshotContentsJSON = nil
	}

	backupInfo := persistence.BackupInfo{
		Name:                      backup.Name,
		Metadata:                  backupJSON,
		Contents:                  backupContents,
		Log:                       backupLog,
		PodVolumeBackups:          podVolumeBackups,
		VolumeSnapshots:           nativeVolumeSnapshots,
		BackupResourceList:        backupResourceList,
		CSIVolumeSnapshots:        csiSnapshotJSON,
		CSIVolumeSnapshotContents: csiSnapshotContentsJSON,
	}
	if err := backupStore.PutBackup(backupInfo); err != nil {
		persistErrs = append(persistErrs, err)
	}

	return persistErrs
}

func closeAndRemoveFile(file *os.File, log logrus.FieldLogger) {
	if file == nil {
		log.Debug("Skipping removal of file due to nil file pointer")
		return
	}
	if err := file.Close(); err != nil {
		log.WithError(err).WithField("file", file.Name()).Error("error closing file")
	}
	if err := os.Remove(file.Name()); err != nil {
		log.WithError(err).WithField("file", file.Name()).Error("error removing file")
	}
}

// encodeToJSONGzip takes arbitrary Go data and encodes it to GZip compressed JSON in a buffer, as well as a description of the data to put into an error should encoding fail.
func encodeToJSONGzip(data interface{}, desc string) (*bytes.Buffer, []error) {
	buf := new(bytes.Buffer)
	gzw := gzip.NewWriter(buf)

	// Since both encoding and closing the gzip writer could fail separately and both errors are useful,
	// collect both errors to report back.
	errs := []error{}

	if err := json.NewEncoder(gzw).Encode(data); err != nil {
		errs = append(errs, errors.Wrapf(err, "error encoding %s", desc))
	}
	if err := gzw.Close(); err != nil {
		errs = append(errs, errors.Wrapf(err, "error closing gzip writer for %s", desc))
	}

	if len(errs) > 0 {
		return nil, errs
	}

	return buf, nil
}
