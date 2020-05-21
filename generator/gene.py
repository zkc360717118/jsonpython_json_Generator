#!/usr/bin/python
# -*- coding: UTF-8 -*-
from generator.Module import Tool
from generator.generatorJson import WorkBench

import json
import time

tool = Tool()
test = WorkBench("LRR_JP_OTC")


# prepare for read stage 输入部分
taskOneForReadStage = tool.getTaskForReadStage(module=test.module,
                                               taskNm="READ_OTC_DATA",
                                               taskDesc="read data from Oracel table",
                                               taskSeq=0,
                                               intputparamVal="DSET.IN_V_ODS_OTC_INDCTV_BAL_M",
                                               dsetDesc="read data from V_ODS_OTC_INDCTV_BAL_D",
                                               qryStr="select * from V_ODS_OTC_INDCTV_BAL_D WHERE ARRG_DIM_COB_DT***********",
                                               # 这里要看看线上乱码
                                               outputparamVal="DSET.OUT_LRR_JP_OTC"
                                               )

taskTwoForReadStage = tool.getTaskForReadStage(module=test.module,
                                               taskNm="BOJ_RATE_LOOKUP",
                                               taskDesc="read data from BOJ_RATE_MAN table",
                                               taskSeq=1,
                                               intputparamVal="DSET.IN_BOJ_RATE_MAN",
                                               dsetDesc="read data from BOJ_RATE_MAN",
                                               qryStr="select CCY, BOJ_RATE,AS_OF_DT from BOJ_RATE_MAN WHERE ***********",
                                               # 这里要看看线上乱码
                                               outputparamVal="DSET.OUT_BOJ_RATE_LOOKUP"
                                               )

test.addSubtasksForReadStage(taskOneForReadStage, taskTwoForReadStage)

# prepare for tranformation stage 转换部分
task1 = tool.getTaskForTransStage(taskType="DEDUP",
                                                   module=test.module,
                                                   taskNm="DEDUP_LRR_JP_OTC_DATA",
                                                   taskDesc="Dedup JPLRR input dataset",
                                                   taskSeq=0,
                                                   depdTask="READ_OTC_DATA",
                                                   processParamVal={"in_data": "DSET.OUT_LRR_JP_OTC",
                                                                    "keys": "SURR_KEY",
                                                                    "out_data": "LRR_JP_OTC_DEDUP_OUT"
                                                                    }
                                                   )

task2 = tool.getTaskForTransStage(taskType="SORT",
                                                   module=test.module,
                                                   taskNm="SORT_LRR_JP_OTC",
                                                   taskDesc="sort data",
                                                   taskSeq=1,
                                                   depdTask="DEDUP_LRR_JP_OTC_DATA",
                                                   processParamVal={"in_data": "LRR_JP_OTC_DEDUP_OUT",
                                                                    "out_data": "LRR_JP_OTC_SORT_OUT",
                                                                    "sort": "UNIQ_ID_IN_SRC_SYS asc,@@@@_LV asc,CPRT_GFCID asc,CPRT_GFCID asc,ACTG_UNIT_ID asc"
                                                                    })

task3TrfmExpression = "{ PREV_TXN_CCY_TEMP1:: LRR_JP_OTC \n ACCTG_UNIT_ID:: DATA_TRANSFORM}"
task3 = tool.getTaskForTransStage(taskType="reformat",
                                                   module=test.module,
                                                   taskNm="REFORMAT_SCAN_LRR_JP_OTC_INDEX1",
                                                   taskDesc="reformat data",
                                                   taskSeq=2,
                                                   depdTask="SORT_LRR_JP_OTC",
                                                   trfmExpression=task3TrfmExpression,
                                                   # outputparamVal="LRR_JP_OTC_REFORMAT_SCAN_INDEX1_OUT",
                                                   processParamVal={"in_data": "LRR_JP_OTC_SORT_OUT",
                                                                    "out_data": "LRR_JP_OTC_REFORMAT_SCAN_INDEX1_OUT",
                                                                    "transform": "LRR_JP_OTC_REFORMAT_SCAN_INDEX1_TRF"
                                                                    }
                                                   )

task4 = tool.getTaskForTransStage(taskType="sort_with_groups",
                                                   module=test.module,
                                                   taskNm="SWG_LRR_JP_OTC",
                                                   taskDesc="sort with groups",
                                                   taskSeq=3,
                                                   depdTask="REFORMAT_SCAN_LRR_JP_OTC_INDEX1",
                                                   processParamVal={"in_data": "LRR_JP_OTC_REFORMAT_SCAN_INDEX1_OUT",
                                                                    "out_data": "LRR_JP_OTC_SWG_OUT",
                                                                    "majorKey": "UNIQ_ID_IN_SRC_SYS,@@@@_LV,CPRT_GFCID",
                                                                    "majorKeyOrder": "asc,asc,asc",
                                                                    "minorKey": "ACTG_UNIT_ID",
                                                                    "minorKeyOrder": "desc"}
                                                   # trfmExpression=task4TrfmExpression,
                                                   )

task5 = tool.getTaskForTransStage(taskType="aggregate",
                                                   module=test.module,
                                                   taskNm="AGGREAGET_LRR_JP_OTC",
                                                   taskDesc="aggregate data",
                                                   taskSeq=4,
                                                   depdTask="LRR_JP_OTC_SWG_OUT",
                                                   processParamVal={"in_data": "LRR_JP_OTC_SWG_OUT",
                                                                    "out_data": "LRR_JP_OTC_AGGREAGET_OUT",
                                                                    "agg": "TXN_CCY_AMT_temp1:SUM~TXN_CCY_AMT_temp2,FNCT_CCY_AMT_temp1:SUM~FNCT_CCY_AMT_temp2",
                                                                    "group_by": "UNIQ_ID_IN_SRC_SYS,@@@@_LV,CPRT_GFCID"}
                                                   )

task6 = tool.getTaskForTransStage(taskType="union",
                                                   module=test.module,
                                                   taskNm="UNION_LRR_JP_OTC",
                                                   taskDesc="union data",
                                                   taskSeq=5,
                                                   depdTask="AGGREAGET_LRR_JP_OTC",
                                                   processParamVal={
                                                       "in_data": "LRR_JP_OTC_REFORMAT_ROLLUP_INDEX4_OUT,LRR_JP_OTC_REFORMAT_SCAN_INDEX3_OUT",
                                                       "out_data": "LRR_JP_OTC_UNION_OUT"
                                                   }
                                                   )


test.addSubtasksForTransformStage(task1,task2,task3,task4,task5,task6)

# 输出
taskOneForReadStage = tool.getTaskForOutputStage(module=test.module,
                                                 taskNm="TARGET_LOAD",
                                                 taskDesc="load data from target table",
                                                 intputparamVal="LRR_JP_OTC_UNION_OUT",
                                                 depdTask="UNION_LRR_JP_OTC",
                                                 tblNm="ODS_ENRICHED_OTC_DATASET",
                                                 dsetDesc="write data to MASTER_DATASET",
                                                 outputparamVal="DSET.OUT_LRR_JP_OTC"
                                                 )
test.addSubtasksForOutputStage(taskOneForReadStage)


#生成并输出json文件

finalJson = test.formFinalJson( procNm ="OTC_DAILY"
                   , subProcNm = "OTC"
                   , description = "FOR OTC DAILY"
                   , createdBy="kz05127"
                    )
with open("output.json","w") as w:
    w.write(json.dumps(finalJson,indent=2))









# task7ForTransformStage = tool.getTaskForTransStage(taskType="lookup",
#                                                    module=test.module,
#                                                    taskNm="LOOKUP_BOJ_RATE",
#                                                    taskDesc="union data",
#                                                    taskSeq=5,
#                                                    depdTask="AGGREAGET_LRR_JP_OTC",
#                                                    processParamVal={
#                                                        "in_data": "LRR_JP_OTC_REFORMAT_ROLLUP_INDEX4_OUT,LRR_JP_OTC_REFORMAT_SCAN_INDEX3_OUT",
#                                                        "out_data": "LRR_JP_OTC_UNION_OUT"
#                                                    }
#                                                    )

# print(json.dumps(task5ForTransformStage))