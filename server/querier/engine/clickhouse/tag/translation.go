package tag

import (
	"fmt"
	"strconv"

	"github.com/metaflowys/metaflow/server/querier/common"
)

var TagResoureMap = GenerateTagResoureMap()
var DEVICE_MAP = map[string]int{
	"chost":       VIF_DEVICE_TYPE_VM,
	"router":      VIF_DEVICE_TYPE_VROUTER,
	"dhcpgw":      VIF_DEVICE_TYPE_DHCP_PORT,
	"pod_service": VIF_DEVICE_TYPE_POD_SERVICE,
	"redis":       VIF_DEVICE_TYPE_REDIS_INSTANCE,
	"rds":         VIF_DEVICE_TYPE_RDS_INSTANCE,
	"lb":          VIF_DEVICE_TYPE_LB,
	"natgw":       VIF_DEVICE_TYPE_NAT_GATEWAY,
}

func GenerateTagResoureMap() map[string]map[string]*Tag {
	tagResourceMap := make(map[string]map[string]*Tag)
	// 资源:区域，可用区，容器节点，命名空间，工作负载，容器POD，容器集群，子网
	for _, resourceStr := range TAG_RESOURCE_TYPE_DEFAULT {
		// 以下分别针对单端/双端-0端/双端-1端生成name和ID的Tag定义
		for _, suffix := range []string{"", "_0", "_1"} {
			resourceIDSuffix := resourceStr + "_id" + suffix
			resourceNameSuffix := resourceStr + suffix
			tagResourceMap[resourceIDSuffix] = map[string]*Tag{
				"default": NewTag(
					"",
					resourceIDSuffix+"!=0",
					"",
					"",
				),
			}
			tagResourceMap[resourceNameSuffix] = map[string]*Tag{
				"default": NewTag(
					"dictGet(deepflow."+resourceStr+"_map, 'name', (toUInt64("+resourceIDSuffix+")))",
					resourceIDSuffix+"!=0",
					"toUInt64("+resourceIDSuffix+") IN (SELECT id FROM deepflow."+resourceStr+"_map WHERE name %s %s)",
					"toUInt64("+resourceIDSuffix+") IN (SELECT id FROM deepflow."+resourceStr+"_map WHERE %s(name,%s))",
				),
				"node_type": NewTag(
					"'"+resourceStr+"'",
					"",
					"",
					"",
				),
				"icon_id": NewTag(
					"dictGet(deepflow."+resourceStr+"_map, 'icon_id', (toUInt64("+resourceIDSuffix+")))",
					"",
					"",
					"",
				),
			}
		}
	}

	// VPC资源
	// 以下分别针对单端/双端-0端/双端-1端生成name和ID的Tag定义
	for _, suffix := range []string{"", "_0", "_1"} {
		// l3_epc
		vpcIDSuffix := "vpc_id" + suffix
		l3EPCIDSuffix := "l3_epc_id" + suffix
		vpcNameSuffix := "vpc" + suffix
		tagResourceMap[vpcIDSuffix] = map[string]*Tag{
			"default": NewTag(
				l3EPCIDSuffix,
				l3EPCIDSuffix+"!=-2",
				l3EPCIDSuffix+" %s %s",
				"",
			)}
		tagResourceMap[vpcNameSuffix] = map[string]*Tag{
			"default": NewTag(
				"dictGet(deepflow.l3_epc_map, 'name', (toUInt64("+l3EPCIDSuffix+")))",
				l3EPCIDSuffix+"!=-2",
				"toUInt64("+l3EPCIDSuffix+") IN (SELECT id FROM deepflow.l3_epc_map WHERE name %s %s)",
				"toUInt64("+l3EPCIDSuffix+") IN (SELECT id FROM deepflow.l3_epc_map WHERE %s(name,%s))",
			),
			"node_type": NewTag(
				"'vpc'",
				"",
				"",
				"",
			),
			"icon_id": NewTag(
				"dictGet(deepflow.l3_epc_map, 'icon_id', (toUInt64("+l3EPCIDSuffix+")))",
				"",
				"",
				"",
			),
		}
		// l2_epc
		l2VpcIDSuffix := "l2_vpc_id" + suffix
		l2EPCIDSuffix := "epc_id" + suffix
		l2VpcNameSuffix := "l2_vpc" + suffix
		tagResourceMap[l2VpcIDSuffix] = map[string]*Tag{
			"default": NewTag(
				l2EPCIDSuffix,
				l2EPCIDSuffix+"!=-2",
				l2EPCIDSuffix+" %s %s",
				"",
			)}
		tagResourceMap[l2VpcNameSuffix] = map[string]*Tag{
			"default": NewTag(
				"dictGet(deepflow.l3_epc_map, 'name', (toUInt64("+l2EPCIDSuffix+")))",
				l2EPCIDSuffix+"!=-2",
				"toUInt64("+l2EPCIDSuffix+") IN (SELECT id FROM deepflow.l3_epc_map WHERE name %s %s)",
				"toUInt64("+l2EPCIDSuffix+") IN (SELECT id FROM deepflow.l3_epc_map WHERE %s(name,%s))",
			),
			"node_type": NewTag(
				"'l2_vpc'",
				"",
				"",
				"",
			),
			"icon_id": NewTag(
				"dictGet(deepflow.l3_epc_map, 'icon_id', (toUInt64("+l2EPCIDSuffix+")))",
				"",
				"",
				"",
			),
		}
	}

	// 宿主机
	// 以下分别针对单端/双端-0端/双端-1端生成name和ID的Tag定义
	for _, suffix := range []string{"", "_0", "_1"} {
		hostIDSuffix := "host_id" + suffix
		hostNameSuffix := "host" + suffix
		tagResourceMap[hostIDSuffix] = map[string]*Tag{
			"default": NewTag(
				"",
				hostIDSuffix+"!=0",
				"",
				"",
			)}
		tagResourceMap[hostNameSuffix] = map[string]*Tag{
			"default": NewTag(
				"dictGet(deepflow.device_map, 'name', (toUInt64(6),toUInt64("+hostIDSuffix+")))",
				hostIDSuffix+"!=0",
				"toUInt64("+hostIDSuffix+") IN (SELECT deviceid FROM deepflow.device_map WHERE name %s %s)",
				"toUInt64("+hostIDSuffix+") IN (SELECT deviceid FROM deepflow.device_map WHERE %s(name,%s))",
			),
			"node_type": NewTag(
				"'host'",
				"",
				"",
				"",
			),
			"icon_id": NewTag(
				"dictGet(deepflow.device_map, 'icon_id', (toUInt64("+hostIDSuffix+")))",
				"",
				"",
				"",
			),
		}
	}

	// device资源
	for resourceStr, deviceTypeValue := range DEVICE_MAP {
		if common.IsValueInSliceString(resourceStr, []string{"pod_service", "natgw", "lb"}) {
			continue
		}
		deviceTypeValueStr := strconv.Itoa(deviceTypeValue)
		// 以下分别针对单端/双端-0端/双端-1端生成name和ID的Tag定义
		for _, suffix := range []string{"", "_0", "_1"} {
			resourceIDSuffix := resourceStr + "_id" + suffix
			deviceIDSuffix := "l3_device_id" + suffix
			deviceTypeSuffix := "l3_device_type" + suffix
			resourceNameSuffix := resourceStr + suffix
			tagResourceMap[resourceIDSuffix] = map[string]*Tag{
				"default": NewTag(
					"if("+deviceTypeSuffix+"="+deviceTypeValueStr+","+deviceIDSuffix+", 0)",
					deviceIDSuffix+"!=0 AND "+deviceTypeSuffix+"="+deviceTypeValueStr,
					deviceIDSuffix+" %s %s AND "+deviceTypeSuffix+"="+deviceTypeValueStr,
					"",
				)}
			tagResourceMap[resourceNameSuffix] = map[string]*Tag{
				"default": NewTag(
					"dictGet(deepflow.device_map, 'name', (toUInt64("+deviceTypeValueStr+"),toUInt64("+deviceIDSuffix+")))",
					deviceIDSuffix+"!=0 AND "+deviceTypeSuffix+"="+deviceTypeValueStr,
					"toUInt64("+deviceIDSuffix+") IN (SELECT deviceid FROM deepflow.device_map WHERE name %s %s) AND "+deviceTypeSuffix+"="+deviceTypeValueStr,
					"toUInt64("+deviceIDSuffix+") IN (SELECT deviceid FROM deepflow.device_map WHERE %s(name,%s)) AND "+deviceTypeSuffix+"="+deviceTypeValueStr,
				),
				"node_type": NewTag(
					"'"+resourceStr+"'",
					"",
					"",
					"",
				),
				"icon_id": NewTag(
					"dictGet(deepflow.device_map, 'icon_id', (toUInt64("+deviceTypeValueStr+"),toUInt64("+deviceIDSuffix+")))",
					"",
					"",
					"",
				),
			}
		}
	}

	// 采集器名称
	tagResourceMap["vtap"] = map[string]*Tag{
		"default": NewTag(
			"dictGet(deepflow.vtap_map, 'name', toUInt64(vtap_id))",
			"",
			"toUInt64(vtap_id) IN (SELECT id FROM deepflow.vtap_map WHERE name %s %s)",
			"toUInt64(vtap_id) IN (SELECT id FROM deepflow.vtap_map WHERE %s(name,%s))",
		),
	}

	// 自动分组
	for _, autoStr := range TAG_RESOURCE_TYPE_AUTO {
		// 以下分别针对单端/双端-0端/双端-1端生成name和ID的Tag定义
		for _, suffix := range []string{"", "_0", "_1"} {
			autoIDSuffix := autoStr + "_id" + suffix
			autoTypeSuffix := autoStr + "_type" + suffix
			autoNameSuffix := autoStr + suffix
			nodeTypeStrSuffix := ""
			for nodeType, autoTypeValue := range AutoMap {
				autoTypeValueStr := strconv.Itoa(autoTypeValue)
				nodeTypeStrSuffix = nodeTypeStrSuffix + autoTypeSuffix + "=" + autoTypeValueStr + ",'" + nodeType + "',"
			}
			// internet IP
			internetIPTypeStr := strconv.Itoa(VIF_DEVICE_TYPE_INTERNET)
			nodeTypeStrSuffix = nodeTypeStrSuffix + autoTypeSuffix + "=" + internetIPTypeStr + ",'internet_ip',"
			// IP
			nodeTypeStrSuffix = nodeTypeStrSuffix + "'ip')"
			switch autoStr {
			case "resource_gl0":
				for nodeType, autoTypeValue := range AutoPodMap {
					autoTypeValueStr := strconv.Itoa(autoTypeValue)
					nodeTypeStrSuffix = autoTypeSuffix + "=" + autoTypeValueStr + ",'" + nodeType + "'," + nodeTypeStrSuffix
				}
			case "resource_gl1":
				for nodeType, autoTypeValue := range AutoPodGroupMap {
					autoTypeValueStr := strconv.Itoa(autoTypeValue)
					nodeTypeStrSuffix = autoTypeSuffix + "=" + autoTypeValueStr + ",'" + nodeType + "'," + nodeTypeStrSuffix
				}
			case "resource_gl2":
				for nodeType, autoTypeValue := range AutoServiceMap {
					autoTypeValueStr := strconv.Itoa(autoTypeValue)
					nodeTypeStrSuffix = autoTypeSuffix + "=" + autoTypeValueStr + ",'" + nodeType + "'," + nodeTypeStrSuffix
				}
			}
			internetIconDictGet := "dictGet(deepflow.device_map, 'icon_id', (toUInt64(63999),toUInt64(63999)))"
			ipIconDictGet := "dictGet(deepflow.device_map, 'icon_id', (toUInt64(64000),toUInt64(64000)))"
			autoIconDictGet := fmt.Sprintf("dictGet(deepflow.device_map, 'icon_id', (toUInt64(%s),toUInt64(%s)))", autoTypeSuffix, autoIDSuffix)
			iconIDStrSuffix := fmt.Sprintf("multiIf(%s=%d,%s,%s=%d,%s,%s)", autoTypeSuffix, VIF_DEVICE_TYPE_INTERNET, internetIconDictGet, autoTypeSuffix, VIF_DEVICE_TYPE_IP, ipIconDictGet, autoIconDictGet)
			nodeTypeStrSuffix = "multiIf(" + nodeTypeStrSuffix
			tagResourceMap[autoNameSuffix] = map[string]*Tag{
				"default": NewTag(
					"dictGet(deepflow.device_map, 'name', (toUInt64("+autoTypeSuffix+"),toUInt64("+autoIDSuffix+")))",
					"",
					"toUInt64("+autoIDSuffix+") IN (SELECT deviceid FROM deepflow.device_map WHERE name %s %s)",
					"toUInt64("+autoIDSuffix+") IN (SELECT deviceid FROM deepflow.device_map WHERE %s(name,%s))",
				),
				"node_type": NewTag(
					nodeTypeStrSuffix,
					"",
					"",
					"",
				),
				"icon_id": NewTag(
					iconIDStrSuffix,
					"",
					"",
					"",
				),
			}
		}
	}

	// IP
	// 以下分别针对单端/双端-0端/双端-1端生成name和ID的Tag定义
	for _, suffix := range []string{"", "_0", "_1"} {
		ipSuffix := "ip" + suffix
		ip4Suffix := "ip4" + suffix
		ip6Suffix := "ip6" + suffix
		tagResourceMap[ipSuffix] = map[string]*Tag{
			"default": NewTag(
				"if(is_ipv4=1, IPv4NumToString("+ip4Suffix+"), IPv6NumToString("+ip6Suffix+"))",
				"",
				"if(is_ipv4=1, hex("+ip4Suffix+"), hex("+ip6Suffix+")) %s %s",
				"",
			), "mask": NewTag(
				"if(is_ipv4, IPv4NumToString(bitAnd("+ip4Suffix+", %v)), IPv6NumToString(bitAnd("+ip6Suffix+", toFixedString(unhex('%s'), 16))))",
				"",
				"",
				"",
			),
			"node_type": NewTag(
				"'ip'",
				"",
				"",
				"",
			),
			"icon_id": NewTag(
				"dictGet(deepflow.device_map, 'icon_id', (toUInt64(0),toUInt64(0)))",
				"",
				"",
				"",
			),
		}
	}

	// 广域网
	// 以下分别针对单端/双端-0端/双端-1端生成name和ID的Tag定义
	for _, suffix := range []string{"", "_0", "_1"} {
		isInternetSuffix := "is_internet" + suffix
		l3EPCIDSuffix := "l3_epc_id" + suffix
		tagResourceMap[isInternetSuffix] = map[string]*Tag{
			"default": NewTag(
				"if("+l3EPCIDSuffix+"=-2,1,0",
				"",
				l3EPCIDSuffix+" %s -2",
				"",
			),
			"node_type": NewTag(
				"'internet'",
				"",
				"",
				"",
			),
			"icon_id": NewTag(
				"dictGet(deepflow.device_map, 'icon_id', (toUInt64(63999),toUInt64(63999)))",
				"",
				"",
				"",
			),
		}
	}

	// 关联资源
	for _, relatedResourceStr := range []string{"pod_service", "pod_ingress", "natgw", "lb", "lb_listener"} {
		// 以下分别针对单端/双端-0端/双端-1端生成name和ID的Tag定义
		for _, suffix := range []string{"", "_0", "_1"} {
			relatedResourceID := relatedResourceStr + "_id"
			relatedResourceName := relatedResourceStr + "_name"
			relatedResourceIDSuffix := relatedResourceID + suffix
			relatedResourceNameSuffix := relatedResourceStr + suffix
			ip4Suffix := "ip4" + suffix
			ip6Suffix := "ip6" + suffix
			l3EPCIDSuffix := "l3_epc_id" + suffix
			idTagTranslator := ""
			nameTagTranslator := ""
			notNullFilter := ""
			deviceIDSuffix := "l3_device_id" + suffix
			deviceTypeSuffix := "l3_device_type" + suffix
			deviceTypeValueStr := strconv.Itoa(DEVICE_MAP[relatedResourceStr])
			if common.IsValueInSliceString(relatedResourceStr, []string{"natgw", "lb"}) {
				idTagTranslator = "if(" + deviceTypeSuffix + "=" + deviceTypeValueStr + "," + deviceIDSuffix + ", 0)"
				nameTagTranslator = "dictGet(deepflow.device_map, 'name', (toUInt64(" + deviceTypeValueStr + "),toUInt64(" + deviceIDSuffix + ")))"
				notNullFilter = deviceIDSuffix + "!=0 AND " + deviceTypeSuffix + "=" + deviceTypeValueStr
				tagResourceMap[relatedResourceNameSuffix] = map[string]*Tag{
					"node_type": NewTag(
						"'"+relatedResourceStr+"'",
						"",
						"",
						"",
					),
					"icon_id": NewTag(
						"dictGet(deepflow.device_map, 'icon_id', (toUInt64("+deviceTypeValueStr+"),toUInt64("+deviceIDSuffix+")))",
						"",
						"",
						"",
					),
					"default": NewTag(
						nameTagTranslator,
						notNullFilter,
						"(if(is_ipv4=1,IPv4NumToString("+ip4Suffix+"),IPv6NumToString("+ip6Suffix+")),toUInt64("+l3EPCIDSuffix+")) IN (SELECT ip,l3_epc_id from deepflow.ip_relation_map WHERE "+relatedResourceName+" %s %s)",
						"(if(is_ipv4=1,IPv4NumToString("+ip4Suffix+"),IPv6NumToString("+ip6Suffix+")),toUInt64("+l3EPCIDSuffix+")) IN (SELECT ip,l3_epc_id from deepflow.ip_relation_map WHERE %s("+relatedResourceName+",%s))",
					),
				}
				tagResourceMap[relatedResourceIDSuffix] = map[string]*Tag{
					"default": NewTag(
						idTagTranslator,
						notNullFilter,
						"(if(is_ipv4=1,IPv4NumToString("+ip4Suffix+"),IPv6NumToString("+ip6Suffix+")),toUInt64("+l3EPCIDSuffix+")) IN (SELECT ip,l3_epc_id from deepflow.ip_relation_map WHERE "+relatedResourceID+" %s %s)",
						"",
					),
				}
			} else if relatedResourceStr == "pod_service" {
				idTagTranslator = "if(" + deviceTypeSuffix + "=" + deviceTypeValueStr + "," + deviceIDSuffix + ", 0)"
				nameTagTranslator = "dictGet(deepflow.device_map, 'name', (toUInt64(" + deviceTypeValueStr + "),toUInt64(" + deviceIDSuffix + ")))"
				notNullFilter = deviceIDSuffix + "!=0 AND " + deviceTypeSuffix + "=" + deviceTypeValueStr
				tagResourceMap[relatedResourceNameSuffix] = map[string]*Tag{
					"node_type": NewTag(
						"'"+relatedResourceStr+"'",
						"",
						"",
						"",
					),
					"icon_id": NewTag(
						"dictGet(deepflow.device_map, 'icon_id', (toUInt64("+deviceTypeValueStr+"),toUInt64("+deviceIDSuffix+")))",
						"",
						"",
						"",
					),
					"default": NewTag(
						nameTagTranslator,
						notNullFilter,
						"((if(is_ipv4=1,IPv4NumToString("+ip4Suffix+"),IPv6NumToString("+ip6Suffix+")),toUInt64("+l3EPCIDSuffix+")) IN (SELECT ip,l3_epc_id from deepflow.ip_relation_map WHERE "+relatedResourceName+" %s %s)) OR (toUInt64(service_id"+suffix+") IN (SELECT pod_service_id from deepflow.ip_relation_map WHERE "+relatedResourceName+" %s %s))",
						"((if(is_ipv4=1,IPv4NumToString("+ip4Suffix+"),IPv6NumToString("+ip6Suffix+")),toUInt64("+l3EPCIDSuffix+")) IN (SELECT ip,l3_epc_id from deepflow.ip_relation_map WHERE %s("+relatedResourceName+",%s))) OR (toUInt64(service_id"+suffix+") IN (SELECT pod_service_id from deepflow.ip_relation_map WHERE %s("+relatedResourceName+",%s)))",
					),
				}
				tagResourceMap[relatedResourceIDSuffix] = map[string]*Tag{
					"default": NewTag(
						idTagTranslator,
						notNullFilter,
						"service_id"+suffix+" %s %s",
						"",
					),
				}
			} else if relatedResourceStr == "pod_ingress" {
				// pod_ingress关联资源包含pod_service
				deviceTypeValueStr = strconv.Itoa(VIF_DEVICE_TYPE_POD_SERVICE)
				tagResourceMap[relatedResourceIDSuffix] = map[string]*Tag{
					"default": NewTag(
						"",
						"",
						"((if(is_ipv4=1,IPv4NumToString("+ip4Suffix+"),IPv6NumToString("+ip6Suffix+")),toUInt64("+l3EPCIDSuffix+")) IN (SELECT ip,l3_epc_id from deepflow.ip_relation_map WHERE "+relatedResourceID+" %s %s)) OR (toUInt64(service_id"+suffix+") IN (SELECT pod_service_id from deepflow.ip_relation_map WHERE "+relatedResourceID+" %s %s))",
						"",
					),
				}
				tagResourceMap[relatedResourceNameSuffix] = map[string]*Tag{
					"default": NewTag(
						"",
						"",
						"((if(is_ipv4=1,IPv4NumToString("+ip4Suffix+"),IPv6NumToString("+ip6Suffix+")),toUInt64("+l3EPCIDSuffix+")) IN (SELECT ip,l3_epc_id from deepflow.ip_relation_map WHERE "+relatedResourceName+" %s %s)) OR (toUInt64(service_id"+suffix+") IN (SELECT pod_service_id from deepflow.ip_relation_map WHERE "+relatedResourceName+" %s %s))",
						"((if(is_ipv4=1,IPv4NumToString("+ip4Suffix+"),IPv6NumToString("+ip6Suffix+")),toUInt64("+l3EPCIDSuffix+")) IN (SELECT ip,l3_epc_id from deepflow.ip_relation_map WHERE %s("+relatedResourceName+",%s))) OR (toUInt64(service_id"+suffix+") IN (SELECT pod_service_id from deepflow.ip_relation_map WHERE %s("+relatedResourceName+",%s)))",
					),
				}
			} else {
				tagResourceMap[relatedResourceIDSuffix] = map[string]*Tag{
					"default": NewTag(
						"",
						"",
						"(if(is_ipv4=1,IPv4NumToString("+ip4Suffix+"),IPv6NumToString("+ip6Suffix+")),toUInt64("+l3EPCIDSuffix+")) IN (SELECT ip,l3_epc_id from deepflow.ip_relation_map WHERE "+relatedResourceID+" %s %s)",
						"",
					),
				}
				tagResourceMap[relatedResourceNameSuffix] = map[string]*Tag{
					"default": NewTag(
						"",
						"",
						"(if(is_ipv4=1,IPv4NumToString("+ip4Suffix+"),IPv6NumToString("+ip6Suffix+")),toUInt64("+l3EPCIDSuffix+")) IN (SELECT ip,l3_epc_id from deepflow.ip_relation_map WHERE "+relatedResourceName+" %s %s)",
						"(if(is_ipv4=1,IPv4NumToString("+ip4Suffix+"),IPv6NumToString("+ip6Suffix+")),toUInt64("+l3EPCIDSuffix+")) IN (SELECT ip,l3_epc_id from deepflow.ip_relation_map WHERE %s("+relatedResourceName+",%s))",
					),
				}
			}
		}
	}

	// vtap对应资源
	vtapResource := "'device_type','device_id','device_name','icon_id','host_id','host_name'"
	gwDictGet := fmt.Sprintf("dictGet(deepflow.vtap_port_map, (%s),(toUInt64(vtap_id),toUInt64(tap_port)))", vtapResource)
	tagResourceMap["resource_from_vtap"] = map[string]*Tag{
		"default": NewTag(
			gwDictGet,
			"",
			"",
			"",
		),
	}

	// k8s标签
	// 以下分别针对单端/双端-0端/双端-1端生成name和ID的Tag定义
	for _, suffix := range []string{"", "_0", "_1"} {
		k8sLabelSuffix := "k8s_label" + suffix
		podIDSuffix := "pod_id" + suffix
		tagResourceMap[k8sLabelSuffix] = map[string]*Tag{
			"default": NewTag(
				"dictGet(deepflow.k8s_label_map, 'value', (toUInt64("+podIDSuffix+"),'%s'))",
				"",
				"toUInt64("+podIDSuffix+") IN (SELECT pod_id FROM deepflow.k8s_label_map WHERE value %s %s and key='%s')",
				"toUInt64("+podIDSuffix+") IN (SELECT pod_id FROM deepflow.k8s_label_map WHERE %s(value,%s) and key='%s')",
			),
		}
	}

	// 单个外部字段-ext_metrics
	tagResourceMap["tag"] = map[string]*Tag{
		"default": NewTag(
			"tag_values[indexOf(tag_names,'%s')]",
			"",
			"tag_values[indexOf(tag_names,'%s')] %s %v",
			"%s(tag_values[indexOf(tag_names,'%s')],%v)",
		),
	}
	// 单个外部字段-l7_flow_log
	tagResourceMap["attribute"] = map[string]*Tag{
		"default": NewTag(
			"attribute_values[indexOf(attribute_names,'%s')]",
			"",
			"attribute_values[indexOf(attribute_names,'%s')] %s %v",
			"%s(attribute_values[indexOf(attribute_names,'%s')],%v)",
		),
	}
	// 外部字段map
	tagResourceMap["tags"] = map[string]*Tag{
		"default": NewTag(
			"arrayZip(tag_names, tag_values)",
			"",
			"",
			"",
		),
	}
	tagResourceMap["attributes"] = map[string]*Tag{
		"default": NewTag(
			"arrayZip(attribute_names, attribute_values)",
			"",
			"",
			"",
		),
	}
	// 采集点ID
	tagResourceMap["tap_id"] = map[string]*Tag{
		"default": NewTag(
			"tap_type",
			"",
			"tap_type %s %s",
			"",
		)}
	// 采集点
	tagResourceMap["tap"] = map[string]*Tag{
		"default": NewTag(
			"dictGet(deepflow.tap_type_map, 'name', toUInt64(tap_type))",
			"",
			"toUInt64(tap_type) IN (SELECT value FROM deepflow.tap_type_map WHERE name %s %s)",
			"toUInt64(tap_type) IN (SELECT value FROM deepflow.tap_type_map WHERE %s(name,%s))",
		)}

	// 响应码
	tagResourceMap["response_code"] = map[string]*Tag{
		"default": NewTag(
			"",
			"isNotNull(response_code)",
			"",
			"",
		)}
	// IP类型
	tagResourceMap["ip_version"] = map[string]*Tag{
		"default": NewTag(
			"if(is_ipv4=1, 4, 6)",
			"",
			"is_ipv4 %s %s",
			"",
		)}
	// _ID
	tagResourceMap["_id"] = map[string]*Tag{
		"default": NewTag(
			"",
			"",
			"_id %s %s AND time=toDateTime(bitShiftRight(%v, 32))",
			"",
		)}
	// 采集位置名称
	tagResourceMap["tap_port_name"] = map[string]*Tag{
		"default": NewTag(
			"if(tap_port_type in (0,1,2),dictGet(deepflow.vtap_port_map, 'name', (toUInt64(vtap_id),toUInt64(tap_port))),'')",
			"",
			"toUInt64(tap_port) IN (SELECT tap_port FROM deepflow.vtap_port_map WHERE name %s %s)",
			"toUInt64(tap_port) IN (SELECT tap_port FROM deepflow.vtap_port_map WHERE %s(name,%s))",
		)}
	// Tunnel IP
	tagResourceMap["tunnel_tx_ip_0"] = map[string]*Tag{
		"default": NewTag(
			"if(tunnel_is_ipv4, IPv4NumToString(tunnel_tx_ip4_0), IPv6NumToString(tunnel_tx_ip6_0))",
			"",
			"if(is_ipv4=1, hex(tunnel_tx_ip4_0), hex(tunnel_tx_ip6_0)) %s %s",
			"",
		)}
	tagResourceMap["tunnel_tx_ip_1"] = map[string]*Tag{
		"default": NewTag(
			"if(tunnel_is_ipv4, IPv4NumToString(tunnel_tx_ip4_1), IPv6NumToString(tunnel_tx_ip6_1))",
			"",
			"if(is_ipv4=1, hex(tunnel_tx_ip4_1), hex(tunnel_tx_ip6_1)) %s %s",
			"",
		)}
	tagResourceMap["tunnel_rx_ip_0"] = map[string]*Tag{
		"default": NewTag(
			"if(tunnel_is_ipv4, IPv4NumToString(tunnel_rx_ip4_0), IPv6NumToString(tunnel_rx_ip6_0))",
			"",
			"if(is_ipv4=1, hex(tunnel_rx_ip4_0), hex(tunnel_rx_ip6_0)) %s %s",
			"",
		)}
	tagResourceMap["tunnel_rx_ip_1"] = map[string]*Tag{
		"default": NewTag(
			"if(tunnel_is_ipv4, IPv4NumToString(tunnel_rx_ip4_1), IPv6NumToString(tunnel_rx_ip6_1))",
			"",
			"if(is_ipv4=1, hex(tunnel_rx_ip4_1), hex(tunnel_rx_ip6_1)) %s %s",
			"",
		)}
	// 开始时间
	tagResourceMap["start_time"] = map[string]*Tag{
		"toString": NewTag(
			"toString(start_time)",
			"",
			"",
			"",
		)}
	// 结束时间
	tagResourceMap["end_time"] = map[string]*Tag{
		"toString": NewTag(
			"toString(end_time)",
			"",
			"",
			"",
		)}
	return tagResourceMap
}
