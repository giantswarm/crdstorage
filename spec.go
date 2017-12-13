package crdstorage

type deletionConfig struct {
	Spec deletionConfigSpec `json:"spec"`
}

type deletionConfigSpec struct {
	Storage deletionConfigSpecStorage `json:"storage" yaml:"storage"`
}

type deletionConfigSpecStorage struct {
	Data map[string]*string `json:"data" yaml:"data"`
}
