package crdstorage

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/cenkalti/backoff"
	"github.com/giantswarm/apiextensions/pkg/apis/core/v1alpha1"
	"github.com/giantswarm/apiextensions/pkg/clientset/versioned"
	"github.com/giantswarm/microerror"
	"github.com/giantswarm/micrologger"
	"github.com/giantswarm/microstorage"
	"github.com/giantswarm/operatorkit/crdclient"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	api "k8s.io/client-go/pkg/api/v1"
)

type Config struct {
	CRDClient crdclient.CRDClient
	G8sClient versioned.Interface
	K8sClient kubernetes.Interface
	Logger    micrologger.Logger

	CRD       *apiextensionsv1beta1.CustomResourceDefinition
	Name      string
	Namespace *api.Namespace
}

func DefaultConfig() Config {
	return Config{
		CRDClient: nil,
		G8sClient: nil,
		K8sClient: nil,
		Logger:    nil,

		CRD:       nil,
		Name:      "",
		Namespace: nil,
	}
}

type Storage struct {
	crdClient crdclient.CRDClient
	g8sClient versioned.Interface
	k8sClient kubernetes.Interface
	logger    micrologger.Logger

	crd       *apiextensionsv1beta1.CustomResourceDefinition
	name      string
	namespace *api.Namespace
}

// New creates an uninitialized instance of Storage. It is required to call Boot
// before running any read/write operations against the returned Storage
// instance.
func New(config Config) (*Storage, error) {
	if config.CRDClient == nil {
		return nil, microerror.Maskf(invalidConfigError, "config.CRDClient must not be empty")
	}
	if config.G8sClient == nil {
		return nil, microerror.Maskf(invalidConfigError, "config.G8sClient must not be empty")
	}
	if config.K8sClient == nil {
		return nil, microerror.Maskf(invalidConfigError, "config.K8sClient must not be empty")
	}
	if config.Logger == nil {
		return nil, microerror.Maskf(invalidConfigError, "config.Logger must not be empty")
	}

	if config.CRD == nil {
		return nil, microerror.Maskf(invalidConfigError, "config.CRD must not be empty")
	}
	if config.Name == nil {
		return nil, microerror.Maskf(invalidConfigError, "config.Name must not be empty")
	}
	if config.Namespace == nil {
		return nil, microerror.Maskf(invalidConfigError, "config.Namespace must not be empty")
	}

	s := &Storage{
		crdClient: config.CRDClient,
		g8sClient: config.G8sClient,
		k8sClient: config.K8sClient,
		logger: config.Logger.With(
			"crdName", config.CRD.Name,
			"crdVersion", config.CRD.Version,
		),

		crd:       config.CRD,
		name:      config.Name,
		namespace: config.Namespace,
	}

	return s, nil
}

// Boot initializes the Storage by ensuring Kubernetes resources used by the
// Storage are in place. It is safe to call Boot more than once.
func (s *Storage) Boot(ctx context.Context) error {
	// Create CRD.
	{
		backOff := backoff.NewExponentialBackOff()
		backOff.MaxElapsedTime = 0
		backOff = backoff.WithMaxTries(backOff, 7)

		err := s.crdClient.Ensure(ctx, s.crd, backOff)
		if err != nil {
			return microerror.Mask(err)
		}
	}

	// Create namespace.
	{
		_, err := s.k8sClient.CoreV1().Namespaces().Create(s.namespace)
		if errors.IsAlreadyExists(err) {
			// TODO logs
		} else if err != nil {
			return microerror.Mask(err)
		} else {
			// TODO logs
		}
	}

	// Create CRO.
	{
		storageConfig := &v1alpha1.StorageConfig{}

		storageConfig.Kind = "StorageConfig"
		storageConfig.APIVersion = "core.giantswarm.io"
		storageConfig.Name = s.name
		storageConfig.Namespace = s.namespace.Name
		storageConfig.Spec.Storage.Data = map[string]string{}

		operation := func() error {
			_, err := s.g8sClient.CoreV1alpha1().StorageConfigs(s.namespace.Name).Create(storageConfig)
			if errors.IsAlreadyExists(err) {
				// TODO logs
			} else if err != nil {
				return microerror.Mask(err)
			} else {
				// TODO logs
			}

			return nil
		}

		backOff := backoff.NewExponentialBackOff()
		backOff.MaxElapsedTime = 0
		backOff = backoff.WithMaxTries(backOff, 7)

		err := backoff.Retry(operation, backOff)
		if err != nil {
			return microerror.Mask(err)
		}
	}

	return nil
}

func (s *Storage) Put(ctx context.Context, kv microstorage.KV) error {
	var err error

	var body []byte
	{
		v := struct {
			Spec v1alpha1.StorageConfigSpec `json:"spec"`
		}{}
		v.Spec.Storage.Data[kv.Key()] = kv.Val()

		body, err = json.Marshal(v)
		if err != nil {
			return microerror.Mask(err)
		}
	}

	_, err := s.g8sClient.CoreV1alpha1().StorageConfigs(s.namespace.Name).Patch(s.name, typey.MergePatchType, body)
	if err != nil {
		return microerror.Mask(err)
	}

	return nil
}

func (s *Storage) Exists(ctx context.Context, k microstorage.K) (bool, error) {
	key := k.Key()

	data, err := s.getData(ctx)
	if err != nil {
		return false, microerror.Maskf(err, "checking existence key=%s", key)
	}

	_, ok := data[key]
	return ok, nil
}

func (s *Storage) Search(ctx context.Context, k microstorage.K) (microstorage.KV, error) {
	key := k.Key()

	data, err := s.getData(ctx)
	if err != nil {
		return microstorage.KV{}, microerror.Maskf(err, "searching for key=%s", key)
	}

	v, ok := data[key]
	if !ok {
		return microstorage.KV{}, microerror.Maskf(notFoundError, "searching for key=%s", key)
	}

	return microstorage.MustKV(microstorage.NewKV(key, v)), nil
}

func (s *Storage) List(ctx context.Context, k microstorage.K) ([]microstorage.KV, error) {
	key := k.Key()

	data, err := s.getData(ctx)
	if err != nil {
		return nil, microerror.Maskf(err, "listing key=%s", key)
	}

	// Special case.
	if key == "/" {
		var list []microstorage.KV
		for k, v := range data {
			list = append(list, microstorage.MustKV(microstorage.NewKV(k, v)))
		}
		return list, nil
	}

	var list []microstorage.KV

	keyLen := len(key)
	for k, v := range data {
		if len(k) <= keyLen+1 {
			continue
		}
		if !strings.HasPrefix(k, key) {
			continue
		}

		// k must be exact match or be separated with /.
		// I.e. /foo is under /foo/bar but not under /foobar.
		if k[keyLen] != '/' {
			continue
		}

		k = k[keyLen+1:]
		list = append(list, microstorage.MustKV(microstorage.NewKV(k, v)))
	}

	return list, nil
}

func (s *Storage) Delete(ctx context.Context, k microstorage.K) error {
	key := k.Key()

	var body []byte
	{
		v := struct {
			Data map[string]*string `json:"data"`
		}{
			Data: map[string]*string{
				key: nil,
			},
		}

		var err error
		body, err = json.Marshal(&v)
		if err != nil {
			return microerror.Maskf(err, "marshaling %#v", v)
		}
	}

	_, err := s.k8sClient.Core().RESTClient().
		Patch(types.MergePatchType).
		Context(ctx).
		AbsPath(s.tpoEndpoint).
		Body(body).
		DoRaw()
	if err != nil {
		return microerror.Maskf(err, "deleting value for key=%s, patch=%s", key, body)
	}

	return nil
}

func (s *Storage) getData(ctx context.Context) (map[string]string, error) {
	res, err := s.k8sClient.Core().RESTClient().
		Get().
		Context(ctx).
		AbsPath(s.tpoEndpoint).
		DoRaw()
	if err != nil {
		return nil, microerror.Maskf(err, "get TPO")
	}

	var v customObject
	err = json.Unmarshal(res, &v)
	if err != nil {
		return nil, microerror.Maskf(err, "unmarshal TPO")
	}

	return v.Data, nil
}
