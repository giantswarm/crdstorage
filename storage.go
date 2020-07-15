package crdstorage

import (
	"context"
	"strings"

	"github.com/giantswarm/apiextensions/pkg/apis/core/v1alpha1"
	"github.com/giantswarm/apiextensions/pkg/clientset/versioned"
	"github.com/giantswarm/backoff"
	"github.com/giantswarm/microerror"
	"github.com/giantswarm/micrologger"
	"github.com/giantswarm/microstorage"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apismetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Config struct {
	G8sClient versioned.Interface
	K8sClient kubernetes.Interface
	Logger    micrologger.Logger

	Name      string
	Namespace *corev1.Namespace
}

type Storage struct {
	g8sClient versioned.Interface
	k8sClient kubernetes.Interface
	logger    micrologger.Logger

	name      string
	namespace *corev1.Namespace
}

// New creates an uninitialized instance of Storage. It is required to call Boot
// before running any read/write operations against the returned Storage
// instance.
func New(config Config) (*Storage, error) {
	if config.G8sClient == nil {
		return nil, microerror.Maskf(invalidConfigError, "%T.G8sClient must not be empty", config)
	}
	if config.K8sClient == nil {
		return nil, microerror.Maskf(invalidConfigError, "%T.K8sClient must not be empty", config)
	}
	if config.Logger == nil {
		return nil, microerror.Maskf(invalidConfigError, "%T.Logger must not be empty", config)
	}
	if config.Name == "" {
		return nil, microerror.Maskf(invalidConfigError, "%T.Name must not be empty", config)
	}
	if config.Namespace == nil {
		return nil, microerror.Maskf(invalidConfigError, "%T.Namespace must not be empty", config)
	}

	s := &Storage{
		g8sClient: config.G8sClient,
		k8sClient: config.K8sClient,
		logger:    config.Logger,

		name:      config.Name,
		namespace: config.Namespace,
	}

	return s, nil
}

// Boot initializes the Storage by ensuring Kubernetes resources used by the
// Storage are in place. It is safe to call Boot more than once.
func (s *Storage) Boot(ctx context.Context) error {
	// Create namespace.
	{
		_, err := s.k8sClient.CoreV1().Namespaces().Create(ctx, s.namespace, apismetav1.CreateOptions{})
		if errors.IsAlreadyExists(err) {
			// no-op
		} else if err != nil {
			return microerror.Mask(err)
		}
	}

	// Create CR.
	{
		data := make(map[string]string)

		storageConfig := &v1alpha1.StorageConfig{
			ObjectMeta: apismetav1.ObjectMeta{
				Name:      s.name,
				Namespace: s.namespace.Name,
			},
			Spec: v1alpha1.StorageConfigSpec{
				Storage: v1alpha1.StorageConfigSpecStorage{
					Data: data,
				},
			},
		}

		operation := func() error {
			_, err := s.g8sClient.CoreV1alpha1().StorageConfigs(s.namespace.Name).Create(ctx, storageConfig, apismetav1.CreateOptions{})
			if errors.IsAlreadyExists(err) {
				// no-op
			} else if err != nil {
				return microerror.Mask(err)
			}

			return nil
		}

		b := backoff.NewMaxRetries(7, backoff.ShortMaxInterval)

		err := backoff.Retry(operation, b)
		if err != nil {
			return microerror.Mask(err)
		}
	}

	return nil
}

func (s *Storage) Delete(ctx context.Context, k microstorage.K) error {
	storageConfig, err := s.g8sClient.CoreV1alpha1().StorageConfigs(s.namespace.Name).Get(ctx, s.name, apismetav1.GetOptions{})
	if err != nil {
		return microerror.Mask(err)
	}

	delete(storageConfig.Spec.Storage.Data, k.Key())

	_, err = s.g8sClient.CoreV1alpha1().StorageConfigs(s.namespace.Name).Update(ctx, storageConfig, apismetav1.UpdateOptions{})
	if err != nil {
		return microerror.Mask(err)
	}

	return nil
}

func (s *Storage) Exists(ctx context.Context, k microstorage.K) (bool, error) {
	storageConfig, err := s.g8sClient.CoreV1alpha1().StorageConfigs(s.namespace.Name).Get(ctx, s.name, apismetav1.GetOptions{})
	if err != nil {
		return false, microerror.Mask(err)
	}

	_, ok := storageConfig.Spec.Storage.Data[k.Key()]
	if ok {
		return true, nil
	}

	return false, nil
}

func (s *Storage) List(ctx context.Context, k microstorage.K) ([]microstorage.KV, error) {
	storageConfig, err := s.g8sClient.CoreV1alpha1().StorageConfigs(s.namespace.Name).Get(ctx, s.name, apismetav1.GetOptions{})
	if err != nil {
		return nil, microerror.Mask(err)
	}

	key := k.Key()

	// Special case.
	if key == "/" {
		var list []microstorage.KV
		for k, v := range storageConfig.Spec.Storage.Data {
			list = append(list, microstorage.MustKV(microstorage.NewKV(k, v)))
		}
		return list, nil
	}

	var list []microstorage.KV

	keyLen := len(key)
	for k, v := range storageConfig.Spec.Storage.Data {
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

func (s *Storage) Put(ctx context.Context, kv microstorage.KV) error {
	storageConfig, err := s.g8sClient.CoreV1alpha1().StorageConfigs(s.namespace.Name).Get(ctx, s.name, apismetav1.GetOptions{})
	if err != nil {
		return microerror.Mask(err)
	}

	if storageConfig.Spec.Storage.Data == nil {
		storageConfig.Spec.Storage.Data = map[string]string{}
	}

	storageConfig.Spec.Storage.Data[kv.Key()] = kv.Val()

	_, err = s.g8sClient.CoreV1alpha1().StorageConfigs(s.namespace.Name).Update(ctx, storageConfig, apismetav1.UpdateOptions{})
	if err != nil {
		return microerror.Mask(err)
	}

	return nil
}

func (s *Storage) Search(ctx context.Context, k microstorage.K) (microstorage.KV, error) {
	storageConfig, err := s.g8sClient.CoreV1alpha1().StorageConfigs(s.namespace.Name).Get(ctx, s.name, apismetav1.GetOptions{})
	if err != nil {
		return microstorage.KV{}, microerror.Mask(err)
	}

	key := k.Key()
	value, ok := storageConfig.Spec.Storage.Data[key]
	if ok {
		return microstorage.MustKV(microstorage.NewKV(key, value)), nil
	}

	return microstorage.KV{}, microerror.Maskf(notFoundError, "no value for key '%s'", key)
}
