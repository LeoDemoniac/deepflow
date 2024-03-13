package config

import (
	"io/ioutil"
	"os"

	logging "github.com/op/go-logging"
	yaml "gopkg.in/yaml.v2"

	"github.com/deepflowio/deepflow/server/ingester/config"
)

var log = logging.MustGetLogger("exporters_config")

const (
	DefaultExportQueueCount = 4
	DefaultExportQueueSize  = 100000
	DefaultExportBatchSize  = 32
)

const (
	NETWORK_1M uint32 = iota
	NETWORK_MAP_1M

	MAX_DATASOURCE_ID
)

var DataSourceStringMap = map[string]uint32{
	"flow_metrics.network.1m":     NETWORK_1M,
	"flow_metrics.network_map.1m": NETWORK_MAP_1M,
}

type TagFilter struct {
	FieldName   string   `yaml:"field-name"`
	Operator    string   `yaml:"operator"`
	FieldValues []string `yaml:"field-values"`
}

// ExporterCfg holds configs of different exporters.
type ExporterCfg struct {
	Protocol     string      `yaml:"protocol"`
	DataSources  []string    `yaml:"data-sources"`
	Endpoints    []string    `yaml:"endpoints"`
	QueueCount   int         `yaml:"queue-count"`
	QueueSize    int         `yaml:"queue-size"`
	BatchSize    int         `yaml:"batch-size"`
	FlusTimeout  int         `yaml:"flush-timeout"`
	TagFilters   []TagFilter `yaml:"tag-filters"`
	ExportFields []string    `yaml:"export-fields"`

	// private configuration
	ExtraHeaders map[string]string `yaml:"extra-headers"`

	// for Otlp l7_flow_log exporter

	// for promemtheus

	// for kafka
}

func (cfg *ExporterCfg) Validate() error {
	if cfg.BatchSize == 0 {
		cfg.BatchSize = DefaultExportBatchSize
	}

	if cfg.QueueCount == 0 {
		cfg.QueueCount = DefaultExportQueueCount
	}
	if cfg.QueueSize == 0 {
		cfg.QueueSize = DefaultExportQueueSize
	}
	return nil
}

type ExportersConfig struct {
	Exporters Config `yaml:"ingester"`
}

type Config struct {
	Base      *config.Config
	Exporters []ExporterCfg `yaml:"exporters"`
}

func (c *Config) Validate() error {
	for i := range c.Exporters {
		if err := c.Exporters[i].Validate(); err != nil {
			return err
		}
	}
	return nil
}

var DefaultOtlpExportDataTypes = []string{"service_info", "tracing_info", "network_layer", "flow_info", "transport_layer", "application_layer", "metrics"}

func bitsToString(bits uint32, strMap map[string]uint32) string {
	ret := ""
	for k, v := range strMap {
		if bits&v != 0 {
			if len(ret) == 0 {
				ret = k
			} else {
				ret = ret + "," + k
			}
		}
	}
	return ret
}

const (
	UNKNOWN_DATA_TYPE = 0

	SERVICE_INFO uint32 = 1 << iota
	TRACING_INFO
	NETWORK_LAYER
	FLOW_INFO
	CLIENT_UNIVERSAL_TAG
	SERVER_UNIVERSAL_TAG
	TUNNEL_INFO
	TRANSPORT_LAYER
	APPLICATION_LAYER
	CAPTURE_INFO
	CLIENT_CUSTOM_TAG
	SERVER_CUSTOM_TAG
	NATIVE_TAG
	METRICS
	K8S_LABEL
)

var exportedDataTypeStringMap = map[string]uint32{
	"service_info":         SERVICE_INFO,
	"tracing_info":         TRACING_INFO,
	"network_layer":        NETWORK_LAYER,
	"flow_info":            FLOW_INFO,
	"client_universal_tag": CLIENT_UNIVERSAL_TAG,
	"server_universal_tag": SERVER_UNIVERSAL_TAG,
	"tunnel_info":          TUNNEL_INFO,
	"transport_layer":      TRANSPORT_LAYER,
	"application_layer":    APPLICATION_LAYER,
	"capture_info":         CAPTURE_INFO,
	"client_custom_tag":    CLIENT_CUSTOM_TAG,
	"server_custom_tag":    SERVER_CUSTOM_TAG,
	"native_tag":           NATIVE_TAG,
	"metrics":              METRICS,
	"k8s_label":            K8S_LABEL,
}

func StringToExportedDataType(str string) uint32 {
	t, ok := exportedDataTypeStringMap[str]
	if !ok {
		log.Warningf("unknown exporter data type: %s", str)
		return UNKNOWN_DATA_TYPE
	}
	return t
}

func ExportedDataTypeBitsToString(bits uint32) string {
	return bitsToString(bits, exportedDataTypeStringMap)
}

func Load(base *config.Config, path string) *Config {
	config := &ExportersConfig{
		Exporters: Config{
			Base: base,
		},
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Info("no config file, use defaults")
		return &config.Exporters
	}
	configBytes, err := ioutil.ReadFile(path)
	if err != nil {
		log.Warning("Read config file error:", err)
		config.Exporters.Validate()
		return &config.Exporters
	}
	if err = yaml.Unmarshal(configBytes, &config); err != nil {
		log.Error("Unmarshal yaml error:", err)
		os.Exit(1)
	}

	if err = config.Exporters.Validate(); err != nil {
		log.Error(err)
		os.Exit(1)
	}
	return &config.Exporters
}
