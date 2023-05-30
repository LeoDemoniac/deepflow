/**
 * Copyright (c) 2023 Yunshan Networks
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

package prometheus

import (
	"context"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/golang/protobuf/proto"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/deepflowio/deepflow/message/controller"
	"github.com/deepflowio/deepflow/message/trident"
	"github.com/deepflowio/deepflow/server/controller/common"
	"github.com/deepflowio/deepflow/server/controller/side/prometheus/cache"
	. "github.com/deepflowio/deepflow/server/controller/side/prometheus/common"
)

var log = logging.MustGetLogger("side.prometheus")

type Encoder struct {
	cache   *cache.Cache
	grpcurl *GRPCURL
}

func NewEncoder() *Encoder {
	return &Encoder{
		cache:   cache.GetSingletonCache(),
		grpcurl: new(GRPCURL),
	}
}

func (e *Encoder) Encode(metrics []*trident.MetricLabelRequest) ([]*trident.MetricLabelResponse, error) {
	if len(metrics) == 0 {
		return e.assembleFully()
	}
	err := e.prepare(metrics)
	if err != nil {
		log.Errorf("prepare error: %+v", err)
		return nil, err
	}
	return e.assemble(metrics)
}

func (e *Encoder) assembleFully() ([]*trident.MetricLabelResponse, error) {
	defer e.cache.Clear()
	err := e.cache.RefreshFully()
	if err != nil {
		return nil, err
	}
	var res []*trident.MetricLabelResponse
	e.cache.MetricName.Get().Range(func(k, v interface{}) bool {
		var labels []*trident.LabelIDResponse
		metricName := k.(string)
		metricID := v.(int)
		for _, labelName := range e.cache.MetricAndAPPLabelLayout.Get()[metricName] {
			labelNameID, ok := e.cache.LabelName.GetIDByName(labelName)
			if !ok {
				err = errors.Errorf("label_name %s not found", labelName)
				return false
			}
			labelValue, ok := e.cache.Label.GetValueByName(labelName)
			if !ok {
				err = errors.Errorf("label_value %s not found", labelValue)
				return false
			}
			labelValueID, ok := e.cache.LabelValue.GetValueID(labelValue)
			if !ok {
				err = errors.Errorf("label_value_id %s not found", labelValue)
				return false
			}
			idx, ok := e.cache.MetricAndAPPLabelLayout.GetIndex(cache.NewLayoutKey(metricName, labelName))
			if !ok {
				err = errors.Errorf("app_label_index (metric_name: %s, app_label_name) not found", metricName, labelName)
				return false
			}
			label := &trident.LabelIDResponse{
				Name:                &labelName,
				Value:               &labelValue,
				NameId:              proto.Uint32(uint32(labelNameID)),
				ValueId:             proto.Uint32(uint32(labelValueID)),
				AppLabelColumnIndex: proto.Uint32(uint32(idx)),
			}
			labels = append(labels, label)
		}
		targetID, ok := e.cache.MetricTarget.GetTargetID(metricName)
		if !ok {
			for n, v := range e.cache.Target.Get()[targetID] {
				nID, ok := e.cache.LabelName.GetIDByName(n)
				if !ok {
					log.Error("labelNameID not found")
					return false
				}
				vID, ok := e.cache.LabelValue.GetValueID(v)
				if !ok {
					log.Error("labelValueID not found")
					return false
				}
				label := &trident.LabelIDResponse{
					Name:                &n,
					Value:               &v,
					NameId:              proto.Uint32(uint32(nID)),
					ValueId:             proto.Uint32(uint32(vID)),
					AppLabelColumnIndex: proto.Uint32(uint32(0)),
				}
				labels = append(labels, label)
			}
		}
		metric := &trident.MetricLabelResponse{
			MetricName: &metricName,
			MetricId:   proto.Uint32(uint32((metricID))),
			TargetId:   proto.Uint32(uint32(targetID)),
			LabelIds:   labels,
		}
		res = append(res, metric)
		return true
	})
	return res, nil
}

func (e *Encoder) prepare(metrics []*trident.MetricLabelRequest) error {
	metricNamesToE := mapset.NewSet[string]()
	labelNamesToE := mapset.NewSet[string]()
	labelValuesToE := mapset.NewSet[string]()
	metricAPPLabelLayoutsToE := mapset.NewSet[cache.LayoutKey]()
	labelsToAdd := mapset.NewSet[cache.LabelKey]()
	metricTargetsToAdd := mapset.NewSet[cache.MetricTargetKey]()
	for _, m := range metrics {
		mn := m.GetMetricName()
		e.tryAppendMetricNameToEncode(metricNamesToE, mn)
		var instanceValue string
		var jobValue string
		for _, l := range m.GetLabels() {
			ln := l.GetName()
			lv := l.GetValue()
			e.tryAppendLabelNameToEncode(labelNamesToE, ln)
			e.tryAppendLabelValueToEncode(labelValuesToE, lv)
			if ln == TargetLabelInstance {
				instanceValue = l.GetValue()
			} else if ln == TargetLabelJob {
				jobValue = l.GetValue()
			} else if !common.Contains(e.cache.Target.GetLabelNames(), ln) {
				e.tryAppendMetricAPPLabelLayoutToEncode(metricAPPLabelLayoutsToE, cache.NewLayoutKey(mn, ln))
			}
			e.tryAppendLabelToAdd(labelsToAdd, ln, lv)
		}
		e.tryAppendMetricTargetToAdd(metricTargetsToAdd, mn, instanceValue, jobValue)
	}

	if metricNamesToE.Cardinality() == 0 && labelNamesToE.Cardinality() == 0 && labelValuesToE.Cardinality() == 0 && metricAPPLabelLayoutsToE.Cardinality() == 0 && labelsToAdd.Cardinality() == 0 && metricTargetsToAdd.Cardinality() == 0 {
		return nil
	}

	syncResp, err := e.grpcurl.Sync(
		&controller.SyncPrometheusRequest{
			MetricNames: metricNamesToE.ToSlice(),
			LabelNames:  labelNamesToE.ToSlice(),
			LabelValues: labelValuesToE.ToSlice(),
			MetricAppLabelLayouts: func(ks []cache.LayoutKey) []*controller.PrometheusMetricAPPLabelLayoutRequest {
				res := make([]*controller.PrometheusMetricAPPLabelLayoutRequest, 0, len(ks))
				for i := range ks {
					res = append(res, &controller.PrometheusMetricAPPLabelLayoutRequest{
						MetricName:   &ks[i].MetricName,
						AppLabelName: &ks[i].LabelName,
					})
				}
				return res
			}(metricAPPLabelLayoutsToE.ToSlice()),
			Labels: func(ks []cache.LabelKey) []*controller.PrometheusLabel {
				res := make([]*controller.PrometheusLabel, 0, len(ks))
				for i := range ks {
					res = append(res, &controller.PrometheusLabel{
						Name:  &ks[i].Name,
						Value: &ks[i].Value,
					})
				}
				return res
			}(labelsToAdd.ToSlice()),
			MetricTargets: func(ks []cache.MetricTargetKey) []*controller.PrometheusMetricTarget {
				res := make([]*controller.PrometheusMetricTarget, 0, len(ks))
				for i := range ks {
					res = append(res, &controller.PrometheusMetricTarget{
						MetricName: &ks[i].MetricName,
						TargetId:   proto.Uint32(uint32(ks[i].TargetID)),
					})
				}
				return res
			}(metricTargetsToAdd.ToSlice()),
		},
	)
	if err != nil {
		return errors.Wrap(err, "grpcurl.Sync")
	}
	eg, ctx := errgroup.WithContext(context.Background())
	AppendErrGroupWithContext(ctx, eg, e.addMetricNameCache, syncResp.GetMetricNames())
	AppendErrGroupWithContext(ctx, eg, e.addLabelNameCache, syncResp.GetLabelNames())
	AppendErrGroupWithContext(ctx, eg, e.addLabelValueCache, syncResp.GetLabelValues())
	AppendErrGroupWithContext(ctx, eg, e.addMetricAPPLabelLayoutCache, syncResp.GetMetricAppLabelLayouts())
	AppendErrGroupWithContext(ctx, eg, e.addLabelCache, syncResp.GetLabels())
	AppendErrGroupWithContext(ctx, eg, e.addMetricTargetCache, syncResp.GetMetricTargets())
	return eg.Wait()
}

func (e *Encoder) assemble(metrics []*trident.MetricLabelRequest) ([]*trident.MetricLabelResponse, error) {
	respMetrics := make([]*trident.MetricLabelResponse, 0, len(metrics))
	for _, m := range metrics {
		mn := m.GetMetricName()
		mni, ok := e.cache.MetricName.GetIDByName(mn)
		if !ok {
			return nil, errors.Errorf("metric_name %s not found", mn)
		}

		var labels []*trident.LabelIDResponse
		for _, l := range m.GetLabels() {
			ln := l.GetName()
			lv := l.GetValue()
			ni, ok := e.cache.LabelName.GetIDByName(ln)
			if !ok {
				return nil, errors.Errorf("label_name %s not found", ln)
			}
			vi, ok := e.cache.LabelValue.GetValueID(lv)
			if !ok {
				return nil, errors.Errorf("label_value %s not found", lv)
			}
			idx, ok := e.cache.MetricAndAPPLabelLayout.GetIndex(cache.NewLayoutKey(mn, ln))
			if !ok && ln != TargetLabelInstance && ln != TargetLabelJob && !common.Contains(e.cache.Target.GetLabelNames(), ln) {
				return nil, errors.Errorf("app_label_index (metric_name: %s, app_label_name) not found", mn, ln)
			}

			rl := &trident.LabelIDResponse{
				Name:                &ln,
				NameId:              proto.Uint32(uint32(ni)),
				Value:               &lv,
				ValueId:             proto.Uint32(uint32(vi)),
				AppLabelColumnIndex: proto.Uint32(uint32(idx)),
			}
			labels = append(labels, rl)
		}

		ti, _ := e.cache.MetricTarget.GetTargetID(mn)
		rm := &trident.MetricLabelResponse{
			MetricName: &mn,
			MetricId:   proto.Uint32(uint32(mni)),
			TargetId:   proto.Uint32(uint32(ti)),
			LabelIds:   labels,
		}
		respMetrics = append(respMetrics, rm)
	}
	return respMetrics, nil
}

func (e *Encoder) tryAppendMetricNameToEncode(toEn mapset.Set[string], name string) {
	if _, ok := e.cache.MetricName.GetIDByName(name); !ok {
		toEn.Add(name)
	}
}

func (e *Encoder) addMetricNameCache(arg ...interface{}) error {
	mns := arg[0].([]*controller.PrometheusMetricName)
	e.cache.MetricName.Add(mns)
	return nil
}

func (e *Encoder) tryAppendLabelNameToEncode(toEn mapset.Set[string], name string) {
	if _, ok := e.cache.LabelName.GetIDByName(name); !ok {
		toEn.Add(name)
	}
}

func (e *Encoder) addLabelNameCache(arg ...interface{}) error {
	lns := arg[0].([]*controller.PrometheusLabelName)
	e.cache.LabelName.Add(lns)
	return nil
}

func (e *Encoder) tryAppendLabelValueToEncode(toEn mapset.Set[string], name string) {
	if _, ok := e.cache.LabelValue.GetValueID(name); !ok {
		toEn.Add(name)
	}
}

func (e *Encoder) addLabelValueCache(arg ...interface{}) error {
	lvs := arg[0].([]*controller.PrometheusLabelValue)
	e.cache.LabelValue.Add(lvs)
	return nil
}

func (e *Encoder) tryAppendMetricAPPLabelLayoutToEncode(toEn mapset.Set[cache.LayoutKey], k cache.LayoutKey) {
	if _, ok := e.cache.MetricAndAPPLabelLayout.GetIndex(k); !ok {
		toEn.Add(k)
	}
}

func (e *Encoder) addMetricAPPLabelLayoutCache(arg ...interface{}) error {
	ls := arg[0].([]*controller.PrometheusMetricAPPLabelLayout)
	e.cache.MetricAndAPPLabelLayout.Add(ls)
	return nil
}

func (e *Encoder) tryAppendLabelToAdd(toAdd mapset.Set[cache.LabelKey], name, value string) {
	if _, ok := e.cache.Label.GetValueByName(name); !ok {
		toAdd.Add(cache.NewLabelKey(name, value))
	}
}

func (e *Encoder) addLabelCache(arg ...interface{}) error {
	ls := arg[0].([]*controller.PrometheusLabel)
	e.cache.Label.Add(ls)
	return nil
}

func (e *Encoder) tryAppendMetricTargetToAdd(toAdd mapset.Set[cache.MetricTargetKey], metricName, ins, job string) {
	if _, ok := e.cache.MetricTarget.GetTargetID(metricName); !ok {
		log.Infof("try add metric_target (metric_name: %s, instance: %s, job: %s)", metricName, ins, job) // TODO remove
		if ti, ok := e.cache.Target.GetTargetID(cache.NewTargetKey(ins, job)); ok {
			toAdd.Add(cache.NewMetricTargetKey(metricName, ti))
		}
	}
}

func (e *Encoder) addMetricTargetCache(arg ...interface{}) error {
	ts := arg[0].([]*controller.PrometheusMetricTarget)
	e.cache.MetricTarget.Add(ts)
	return nil
}
