module github.com/giantswarm/crdstorage/v2

go 1.14

require (
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/giantswarm/apiextensions/v3 v3.14.1
	github.com/giantswarm/backoff v0.2.0
	github.com/giantswarm/k8sclient/v4 v4.0.0
	github.com/giantswarm/microerror v0.3.0
	github.com/giantswarm/micrologger v0.3.1
	github.com/giantswarm/microstorage v0.2.0
	github.com/juju/errgo v0.0.0-20140925100237-08cceb5d0b53
	k8s.io/api v0.18.9
	k8s.io/apiextensions-apiserver v0.18.9
	k8s.io/apimachinery v0.18.9
	k8s.io/client-go v0.18.9
)

replace sigs.k8s.io/cluster-api => github.com/giantswarm/cluster-api v0.3.10-gs