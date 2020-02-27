import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e57af12515f80e674efc1ee','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	PredictingHighestIncome_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e57af12515f80e674efc1ee", spark, "{'url': '/Demo/PredictHighestIncomeTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi0ef076722999cf4cd8859e9aafdb7b76', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e57af12515f80e674efc1ee','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e57af12515f80e674efc1ee','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e57af12515f80e674efc1ef','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	PredictingHighestIncome_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e57af12515f80e674efc1ee"],{"5e57af12515f80e674efc1ee": PredictingHighestIncome_DBFS}, "5e57af12515f80e674efc1ef", spark,json.dumps( {"FE": [{"transformationsData": {"feature_label": "Occupation"}, "feature": "Occupation", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "558", "mean": "", "stddev": "", "min": "AGRICULTURAL", "max": "Writers and authors", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {"feature_label": "All_weekly"}, "feature": "All_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "558", "mean": "910.05", "stddev": "353.26", "min": "1000", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "M_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "558", "mean": "326.59", "stddev": "2654.6", "min": "0", "max": "60746", "missing": "0"}}, {"transformationsData": {"feature_label": "M_weekly"}, "feature": "M_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "558", "mean": "1002.69", "stddev": "398.31", "min": "1001", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "F_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "558", "mean": "259.83", "stddev": "2142.55", "min": "0", "max": "48334", "missing": "0"}}, {"transformationsData": {"feature_label": "F_weekly"}, "feature": "F_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "558", "mean": "805.43", "stddev": "305.5", "min": "1002", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "All_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "558", "mean": "586.46", "stddev": "4758.98", "min": "0", "max": "109080", "missing": "0"}}, {"feature": "Occupation_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "558", "mean": "278.5", "stddev": "161.22", "min": "0.0", "max": "557.0", "missing": "0"}}, {"feature": "All_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "558", "mean": "70.08", "stddev": "88.17", "min": "0.0", "max": "276.0", "missing": "0"}}, {"feature": "M_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "558", "mean": "40.5", "stddev": "63.44", "min": "0.0", "max": "211.0", "missing": "0"}}, {"feature": "F_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "558", "mean": "29.93", "stddev": "52.35", "min": "0.0", "max": "182.0", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e57af12515f80e674efc1ef','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e57af12515f80e674efc1ef','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e57af12515f80e674efc1f0','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	PredictingHighestIncome_AutoML = tpot_execution.Tpot_execution.run(["5e57af12515f80e674efc1ef"],{"5e57af12515f80e674efc1ef": PredictingHighestIncome_AutoFE}, "5e57af12515f80e674efc1f0", spark,json.dumps( {"model_type": "regression", "label": "All_workers", "features": ["Occupation", "All_weekly", "M_workers", "M_weekly", "F_workers", "F_weekly"], "percentage": "100", "executionTime": "5", "sampling": "0", "sampling_value": "", "run_id": "2b36ec7c31d04da2afbd3631b4563b53", "model_id": "5e57b2109385da042b27cf48", "ProjectName": "ML Sample Problems", "PipelineName": "PredictingHighestIncome", "userid": "5df78f4be2f2eff24740bbd7", "runid": "", "url_ResultView": "http://13.68.212.36:3200", "experiment_id": "480623611921769"}))

	PipelineNotification.PipelineNotification().completed_notification('5e57af12515f80e674efc1f0','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e57af12515f80e674efc1f0','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)

