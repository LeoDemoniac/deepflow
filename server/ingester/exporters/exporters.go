/*
 * Copyright (c) 2024 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package exporters

import (
	logging "github.com/op/go-logging"

	"github.com/deepflowio/deepflow/server/ingester/exporters/common"
	"github.com/deepflowio/deepflow/server/ingester/exporters/config"
	"github.com/deepflowio/deepflow/server/ingester/exporters/otlp_exporter"
	"github.com/deepflowio/deepflow/server/ingester/exporters/universal_tag"
	"github.com/deepflowio/deepflow/server/libs/queue"
)

var log = logging.MustGetLogger("exporters")

const (
	PUT_BATCH_SIZE = 1024
)

type Exporter interface {
	// Starts an exporter worker
	Start()
	// Close an exporter worker
	Close()

	// Put sends data to the exporter worker. Worker could decide what to do next. e.g.:
	// - send it out synchronously.
	// - store it in a queue and handle it later.
	Put(items ...interface{})

	// IsExportData tell the decoder if data need to be sended to specific exporter.
	//	IsExportData(item ExportItem) bool
}

type ExportersCache []interface{}

type Exporters struct {
	config               *config.Config
	universalTagsManager *universal_tag.UniversalTagsManager
	exporters            []Exporter
	dataSourceExporters  [][]Exporter
	putCaches            []ExportersCache // cache for batch put to exporter, multi flowlog decoders call Put(), and put to multi exporters
}

func NewExporters(cfg *config.Config) *Exporters {
	if len(cfg.Exporters) == 0 {
		log.Infof("exporters is empty")
		return nil
	}
	log.Infof("init exporters: %v", cfg.Exporters)
	exporters := make([]Exporter, 0)
	dataSourceExporters := [][]Exporter{}
	putCaches := make([]ExportersCache, config.MAX_DATASOURCE_ID*queue.MAX_QUEUE_COUNT)

	universalTagManager := universal_tag.NewUniversalTagsManager("xxxx", cfg.Base)

	for i, exporterCfg := range cfg.Exporters {
		switch exporterCfg.Protocol {
		case "opentelemetry":
			otlpExporter := otlp_exporter.NewOtlpExporter(i, &exporterCfg, universalTagManager)
			exporters = append(exporters, otlpExporter)
			for _, dataSource := range exporterCfg.DataSources {
				dataSourceId := config.DataSourceStringMap[dataSource]
				dataSourceExporters[dataSourceId] = append(dataSourceExporters[dataSourceId], otlpExporter)
			}
		case "prometheus":
		case "kafka":
		default:
			log.Warningf("unsupport")
		}
	}

	return &Exporters{
		config:               cfg,
		universalTagsManager: universalTagManager,
		exporters:            exporters,
		putCaches:            putCaches,
		dataSourceExporters:  dataSourceExporters,
	}
}

func (es *Exporters) Start() {
	es.universalTagsManager.Start()
	for _, e := range es.exporters {
		e.Start()
	}
}

func (es *Exporters) Close() {
	es.universalTagsManager.Close()
	for _, e := range es.exporters {
		e.Close()
	}
}

func (es *Exporters) IsExportData(common.ExportItem) bool {
	return true
}

// parallel put
func (es *Exporters) Put(dataSourceId, decoderIndex int, item common.ExportItem) {
	if dataSourceId >= len(es.dataSourceExporters) ||
		es.dataSourceExporters[dataSourceId] == nil {
		return
	}
	if item == nil {
		es.Flush(dataSourceId, decoderIndex)
		return
	}
	exporters := es.dataSourceExporters[dataSourceId]
	exportersCount := len(exporters)
	if exportersCount == 0 {
		return
	}
	item.AddReferenceCountN(exportersCount)
	exportersCache := &es.putCaches[dataSourceId*queue.MAX_QUEUE_COUNT+decoderIndex]
	*exportersCache = append(*exportersCache, item)
	for _, e := range exporters {
		if !es.IsExportData(item) {
			item.Release()
			continue
		}
		if len(*exportersCache) >= PUT_BATCH_SIZE {
			e.Put(*exportersCache...)
		}
	}
	*exportersCache = (*exportersCache)[:0]
}

func (es *Exporters) Flush(dataSourceId, decoderIndex int) {
	exportersCache := &es.putCaches[dataSourceId*queue.MAX_QUEUE_COUNT+decoderIndex]

	exporters := es.dataSourceExporters[dataSourceId]
	exportersCount := len(exporters)
	if exportersCount == 0 {
		return
	}
	for _, e := range exporters {
		if len(*exportersCache) >= 0 {
			e.Put(*exportersCache...)
		}
	}
	*exportersCache = (*exportersCache)[:0]
}
